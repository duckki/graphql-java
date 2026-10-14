package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.ResultPath;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Internal
public final class IncrementalWork {
    private final IncrementalExecutionControl control;
    private final List<DeliveryGroup> groups = new ArrayList<>();
    private final List<DeferredTask> tasks = new ArrayList<>();
    private boolean disposed;
    private boolean adopted;

    public IncrementalWork() {
        this(new IncrementalExecutionControl());
    }

    private IncrementalWork(IncrementalExecutionControl control) {
        this.control = control;
    }

    public IncrementalWork newChild() {
        return new IncrementalWork(control);
    }
    public IncrementalExecutionControl getControl() {
        return control;
    }

    public synchronized void checkRunning() {
        control.checkRunning();
        if (disposed || adopted) throw new java.util.concurrent.CancellationException("Incremental scope was disposed");
    }

    public synchronized void addGroup(DeliveryGroup group) {
        if (!disposed && !adopted) groups.add(group);
    }
    public synchronized void addTask(DeferredTask task) {
        if (disposed || adopted) { task.abort(); return; }
        tasks.add(task);
    }
    public synchronized List<DeliveryGroup> getGroups() {
        return new ArrayList<>(groups);
    }
    public synchronized List<DeferredTask> getTasks() {
        return new ArrayList<>(tasks);
    }
    public synchronized void adopt() {
        adopted = true;
        clear();
    }

    public synchronized void dispose() {
        if (disposed || adopted) return;
        disposed = true;
        tasks.forEach(DeferredTask::abort);
        clear();
    }

    public synchronized void filter(@Nullable Object data, ResultPath basePath) {
        groups.removeIf(group -> !exposed(data, basePath, group.getPath()));
        tasks.removeIf(task -> disposeIfHidden(data, basePath, task));
    }

    private boolean disposeIfHidden(@Nullable Object data, ResultPath basePath, DeferredTask task) {
        if (exposed(data, basePath, task.getPath())) return false;
        task.abort();
        return true;
    }

    private boolean exposed(@Nullable Object data, ResultPath basePath, ResultPath path) {
        Object current = data;
        List<Object> segments = path.toList();
        for (int index = basePath.toList().size(); index < segments.size(); index++) {
            current = child(current, segments.get(index));
            if (current == null) return false;
        }
        return current != null;
    }

    private @Nullable Object child(@Nullable Object value, Object segment) {
        if (value instanceof Map) return ((Map<?, ?>) value).get(segment);
        if (value instanceof List && segment instanceof Integer) {
            List<?> values = (List<?>) value;
            int index = (Integer) segment;
            return index < values.size() ? values.get(index) : null;
        }
        return null;
    }

    private void clear() {
        groups.clear();
        tasks.clear();
    }
}
