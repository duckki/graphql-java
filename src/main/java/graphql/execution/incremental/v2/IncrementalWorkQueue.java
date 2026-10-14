package graphql.execution.incremental.v2;

import graphql.GraphQLError;
import graphql.Internal;
import graphql.incremental.DelayedIncrementalPartialResult;
import graphql.incremental.PendingPayload;
import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;

@Internal
public final class IncrementalWorkQueue {
    private final Set<DeliveryGroup> roots = new LinkedHashSet<>();
    private final Set<DeliveryGroup> ready = new LinkedHashSet<>();
    private final Set<ItemStream> streams = new LinkedHashSet<>();
    private final Set<DeliveryGroup> failedGroups = Collections.newSetFromMap(new WeakHashMap<>());
    private final Map<DeliveryGroup, GroupRecord> groupRecords = new LinkedHashMap<>();
    private final Map<DeferredTask, TaskRecord> taskRecords = new LinkedHashMap<>();
    private final Deque<java.util.function.Consumer<IncrementalBatch>> events = new ArrayDeque<>();
    private final IncrementalExecutionControl control;
    private final IncrementalPublisher mapper = new IncrementalPublisher();
    private final IncrementalSerialExecutor executor;
    private final IncrementalResultPublisher publisher;
    private final List<PendingPayload> initialPending = new ArrayList<>();
    private volatile boolean stopped;
    private boolean started;

    public IncrementalWorkQueue(IncrementalWork work, Runnable onFinished) {
        control = work.getControl();
        executor = new IncrementalSerialExecutor(this::fail);
        publisher = new IncrementalResultPublisher(this, onFinished);
        IntegratedWork integrated = integrate(work, null);
        for (DeliveryGroup group : pruneEmpty(integrated.groups)) {
            addRoot(group);
            initialPending.add(mapper.pending(group));
        }
        for (ItemStream stream : integrated.streams) {
            streams.add(stream);
            initialPending.add(mapper.pending(stream));
        }
        integrated.rejectedTasks.forEach(DeferredTask::abort);
    }

    public List<PendingPayload> getInitialPending() {
        return List.copyOf(initialPending);
    }
    public IncrementalResultPublisher getPublisher() {
        return publisher;
    }
    public void dispatch(Runnable command) {
        executor.execute(command);
    }
    public boolean isStopped() {
        return stopped;
    }

    public void start() {
        if (started || stopped) return;
        started = true;
        new ArrayList<>(roots).forEach(this::startGroup);
        new ArrayList<>(streams).forEach(this::startStream);
    }

    public @Nullable DelayedIncrementalPartialResult nextBatch() {
        if (stopped) return null;
        IncrementalBatch batch = new IncrementalBatch();
        int count = events.size();
        for (int index = 0; index < count && !stopped; index++) events.removeFirst().accept(batch);
        drainReady(batch);
        boolean hasNext = !roots.isEmpty() || !streams.isEmpty();
        if (stopped || batch.isEmpty() && hasNext) return null;
        if (!hasNext) retire();
        return batch.toResult(hasNext);
    }

    public void cancel() {
        control.stop();
        stopped = true;
        dispatch(this::cancelRecords);
    }

    private void cancelRecords() {
        Set<DeferredTask> tasks = new LinkedHashSet<>();
        groupRecords.values().forEach(record -> tasks.addAll(record.tasks));
        tasks.forEach(this::disposeTask);
        new ArrayList<>(streams).forEach(ItemStream::cancel);
        retire();
    }

    private void retire() {
        control.stop();
        stopped = true;
        roots.clear();
        ready.clear();
        streams.clear();
        groupRecords.clear();
        taskRecords.clear();
        failedGroups.clear();
        events.clear();
        initialPending.clear();
        mapper.finish();
    }

    private void fail(Throwable exception) {
        cancel();
        publisher.fail(exception);
    }

    private IntegratedWork integrate(IncrementalWork work, @Nullable DeferredTask producer) {
        IntegratedWork integrated = new IntegratedWork();
        List<DeliveryGroup> groups = work.getGroups();
        Set<DeliveryGroup> declared = new LinkedHashSet<>(groups);
        groups.forEach(group -> addGroup(group, declared, integrated, producer));
        work.getTasks().forEach(task -> addTask(task, integrated));
        addStreams(work.getStreams(), producer, integrated);
        work.adopt();
        return integrated;
    }

    private void addGroup(DeliveryGroup group, Set<DeliveryGroup> declared, IntegratedWork integrated, @Nullable DeferredTask producer) {
        Deque<DeliveryGroup> stack = new ArrayDeque<>();
        DeliveryGroup current = group;
        while (current != null && !groupRecords.containsKey(current) && !failedGroups.contains(current)) {
            stack.push(current);
            DeliveryGroup parent = current.getParent();
            current = declared.contains(parent) ? parent : null;
        }
        while (!stack.isEmpty()) installGroup(stack.pop(), integrated, producer);
    }

    private void installGroup(DeliveryGroup group, IntegratedWork integrated, @Nullable DeferredTask producer) {
        DeliveryGroup parent = group.getParent();
        if (failedGroups.contains(parent)) { failedGroups.add(group); return; }
        groupRecords.put(group, new GroupRecord());
        GroupRecord parentRecord = groupRecords.get(parent);
        if (parentRecord != null) parentRecord.children.add(group);
        else if (producer == null) integrated.groups.add(group);
    }

    private void addTask(DeferredTask task, IntegratedWork integrated) {
        boolean registered = false;
        boolean active = false;
        for (DeliveryGroup owner : task.getGroups()) {
            GroupRecord record = groupRecords.get(owner);
            if (record == null) continue;
            registered = true;
            record.tasks.add(task);
            record.pending++;
            if (!record.failed) ready.remove(owner);
            active |= roots.contains(owner);
        }
        if (active) integrated.activeTasks.add(task);
        else if (!registered) integrated.rejectedTasks.add(task);
    }

    private void addStreams(List<ItemStream> newStreams, @Nullable DeferredTask producer, IntegratedWork integrated) {
        if (producer == null) { integrated.streams.addAll(newStreams); return; }
        TaskRecord record = taskRecords.get(producer);
        if (record != null) record.streams.addAll(newStreams);
    }

    private List<DeliveryGroup> pruneEmpty(List<DeliveryGroup> groups) {
        List<DeliveryGroup> result = new ArrayList<>();
        Deque<DeliveryGroup> stack = new ArrayDeque<>();
        pushReverse(stack, groups);
        while (!stack.isEmpty()) pruneGroup(stack.pop(), stack, result);
        return result;
    }

    private void pruneGroup(DeliveryGroup group, Deque<DeliveryGroup> stack, List<DeliveryGroup> result) {
        GroupRecord record = groupRecords.get(group);
        if (record == null) return;
        if (!record.tasks.isEmpty() || record.failed) { result.add(group); return; }
        groupRecords.remove(group);
        ready.remove(group);
        pushReverse(stack, record.children);
    }

    private void pushReverse(Deque<DeliveryGroup> stack, List<DeliveryGroup> groups) {
        for (int index = groups.size() - 1; index >= 0; index--) stack.push(groups.get(index));
    }

    private void addRoot(DeliveryGroup group) {
        roots.add(group);
        GroupRecord record = groupRecords.get(group);
        if (record.pending == 0 || record.failed) ready.add(group);
    }

    private void startNew(IntegratedWork integrated, IncrementalBatch batch) {
        if (stopped) return;
        List<DeliveryGroup> groups = pruneEmpty(integrated.groups);
        for (DeliveryGroup group : groups) { addRoot(group); batch.pending.add(mapper.pending(group)); }
        for (ItemStream stream : integrated.streams) { streams.add(stream); batch.pending.add(mapper.pending(stream)); }
        integrated.rejectedTasks.forEach(DeferredTask::abort);
        integrated.activeTasks.forEach(this::startTask);
        groups.forEach(this::startGroup);
        integrated.streams.forEach(this::startStream);
    }

    private void startGroup(DeliveryGroup group) {
        GroupRecord record = groupRecords.get(group);
        if (record == null || record.failed || stopped) return;
        new ArrayList<>(record.tasks).forEach(this::startTask);
    }

    private void startTask(DeferredTask task) {
        if (stopped || taskRecords.containsKey(task)) return;
        taskRecords.put(task, new TaskRecord());
        task.result().whenComplete((result, exception) -> dispatch(() -> receiveTask(task, result, exception)));
    }

    private void receiveTask(DeferredTask task, @Nullable WorkResult result, @Nullable Throwable exception) {
        if (stopped || !taskRecords.containsKey(task)) {
            if (result != null) result.getWork().dispose();
            return;
        }
        if (exception != null) { fail(exception); return; }
        events.add(batch -> taskCompleted(task, result, batch));
        publisher.pump();
    }

    private boolean healthy(DeliveryGroup group) {
        DeliveryGroup current = group;
        while (current != null) {
            if (failedGroups.contains(current)) return false;
            GroupRecord record = groupRecords.get(current);
            if (record != null && record.failed) return false;
            current = current.getParent();
        }
        return groupRecords.containsKey(group);
    }

    private void taskCompleted(DeferredTask task, WorkResult result, IncrementalBatch batch) {
        TaskRecord record = taskRecords.get(task);
        if (record == null) { result.getWork().dispose(); return; }
        if (task.getGroups().stream().noneMatch(this::healthy)) { disposeTask(task); removeTask(task); return; }
        if (result.isFailed()) { taskFailed(task, result.getErrors(), batch); return; }
        record.result = result;
        IntegratedWork integrated = integrate(result.getWork(), task);
        for (DeliveryGroup owner : task.getGroups()) completeOwner(owner, batch);
        startNew(integrated, batch);
        drainReady(batch);
    }

    private void completeOwner(DeliveryGroup group, IncrementalBatch batch) {
        GroupRecord record = groupRecords.get(group);
        if (record == null) return;
        record.pending--;
        if (roots.contains(group) && record.pending == 0 && !record.failed) finishGroup(group, batch);
    }

    private void taskFailed(DeferredTask task, List<GraphQLError> errors, IncrementalBatch batch) {
        removeTask(task);
        for (DeliveryGroup group : task.getGroups()) failOwner(group, errors, batch);
    }

    private void failOwner(DeliveryGroup group, List<GraphQLError> errors, IncrementalBatch batch) {
        GroupRecord record = groupRecords.get(group);
        if (record == null) return;
        if (roots.contains(group)) { failGroup(group, errors, batch); return; }
        record.pending--;
        record.failed = true;
        record.errors.addAll(errors);
    }

    private void drainReady(IncrementalBatch batch) {
        while (!ready.isEmpty() && !stopped) {
            DeliveryGroup group = ready.iterator().next();
            ready.remove(group);
            GroupRecord record = groupRecords.get(group);
            if (record == null) continue;
            if (record.failed) failGroup(group, record.errors, batch);
            else finishGroup(group, batch);
        }
    }

    private void finishGroup(DeliveryGroup group, IncrementalBatch batch) {
        GroupRecord record = groupRecords.remove(group);
        ready.remove(group);
        roots.remove(group);
        IntegratedWork released = new IntegratedWork();
        for (DeferredTask task : record.tasks) publishTask(group, task, batch, released);
        batch.completed.add(mapper.completed(group, Collections.emptyList()));
        released.groups.addAll(pruneEmpty(record.children));
        startNew(released, batch);
    }

    private void publishTask(DeliveryGroup group, DeferredTask task, IncrementalBatch batch, IntegratedWork released) {
        TaskRecord record = taskRecords.get(task);
        if (record == null) return;
        if (record.result != null) batch.incremental.add(mapper.deferred(group, task, record.result));
        released.streams.addAll(record.streams);
        removeTask(task);
    }

    private void failGroup(DeliveryGroup group, List<GraphQLError> errors, IncrementalBatch batch) {
        List<GraphQLError> reported = List.copyOf(errors);
        removeGroup(group);
        batch.completed.add(mapper.completed(group, reported));
    }

    private void removeGroup(DeliveryGroup group) {
        Deque<DeliveryGroup> stack = new ArrayDeque<>();
        stack.push(group);
        while (!stack.isEmpty()) removeGroupNode(stack.pop(), stack);
    }

    private void removeGroupNode(DeliveryGroup group, Deque<DeliveryGroup> stack) {
        GroupRecord record = groupRecords.remove(group);
        if (record == null) return;
        failedGroups.add(group);
        ready.remove(group);
        roots.remove(group);
        record.tasks.forEach(this::disposeIfOwnerless);
        pushReverse(stack, record.children);
    }

    private void disposeIfOwnerless(DeferredTask task) {
        if (task.getGroups().stream().anyMatch(groupRecords::containsKey)) return;
        disposeTask(task);
        removeTask(task);
    }

    private void disposeTask(DeferredTask task) {
        TaskRecord record = taskRecords.get(task);
        task.abort();
        if (record != null) record.streams.forEach(ItemStream::cancel);
    }

    private void removeTask(DeferredTask task) {
        task.getGroups().forEach(group -> unlinkTask(group, task));
        taskRecords.remove(task);
    }

    private void unlinkTask(DeliveryGroup group, DeferredTask task) {
        GroupRecord record = groupRecords.get(group);
        if (record != null) record.tasks.remove(task);
    }

    private void startStream(ItemStream stream) {
        if (stopped || !streams.contains(stream)) return;
        CompletableFuture.runAsync(() -> pullStream(stream));
    }

    private void pullStream(ItemStream stream) {
        try {
            CompletableFuture<WorkResult> result = stream.next();
            if (result == null) { dispatch(() -> receiveStreamEnd(stream)); return; }
            result.whenComplete((value, exception) -> dispatch(() -> receiveStream(stream, value, exception)));
        } catch (Exception exception) {
            dispatch(() -> receiveStream(stream, null, exception));
        }
    }

    private void receiveStreamEnd(ItemStream stream) {
        if (stopped || !streams.contains(stream)) return;
        events.add(batch -> finishStream(stream, Collections.emptyList(), batch));
        publisher.pump();
    }

    private void receiveStream(ItemStream stream, @Nullable WorkResult result, @Nullable Throwable exception) {
        if (stopped || !streams.contains(stream)) {
            if (result != null) result.getWork().dispose();
            stream.consumed();
            return;
        }
        if (exception != null) events.add(batch -> finishStream(stream, IncrementalErrors.from(exception, stream.getPath()), batch));
        else events.add(batch -> admitItem(stream, result, batch));
        publisher.pump();
    }

    private void admitItem(ItemStream stream, WorkResult result, IncrementalBatch batch) {
        try {
            if (stopped || !streams.contains(stream)) { result.getWork().dispose(); return; }
            if (result.isFailed()) { stream.cancel(); finishStream(stream, result.getErrors(), batch); return; }
            IntegratedWork integrated = integrate(result.getWork(), null);
            batch.incremental.add(mapper.streamed(stream, result));
            startNew(integrated, batch);
        } finally {
            stream.consumed();
        }
        drainReady(batch);
        startStream(stream);
    }

    private void finishStream(ItemStream stream, List<GraphQLError> errors, IncrementalBatch batch) {
        if (!streams.remove(stream)) return;
        if (!errors.isEmpty()) stream.cancel();
        batch.completed.add(mapper.completed(stream, errors));
    }
}
