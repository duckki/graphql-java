package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.ResultPath;
import org.jspecify.annotations.Nullable;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Internal
public final class ItemStream {
    private final ResultPath path;
    private final @Nullable String label;
    private final boolean labelPresent;
    private final Iterator<?> iterator;
    private final StreamItemCompletion completeItem;
    private final IncrementalWork producerWork;
    private final @Nullable AutoCloseable resource;
    private final AtomicBoolean closed = new AtomicBoolean();
    private @Nullable IncrementalWork pendingWork;
    private int index;
    private boolean cancelled;

    public ItemStream(ResultPath path, @Nullable String label, Iterator<?> iterator, int index,
                       @Nullable AutoCloseable resource, IncrementalWork producerWork, StreamItemCompletion completeItem) {
        this(path, label, label != null, iterator, index, resource, producerWork, completeItem);
    }

    public ItemStream(ResultPath path, @Nullable String label, boolean labelPresent, Iterator<?> iterator, int index,
                       @Nullable AutoCloseable resource, IncrementalWork producerWork, StreamItemCompletion completeItem) {
        this.path = path;
        this.label = label;
        this.labelPresent = labelPresent;
        this.iterator = iterator;
        this.index = index;
        this.resource = resource;
        this.completeItem = completeItem;
        this.producerWork = producerWork;
    }

    public ResultPath getPath() { return path; }
    public @Nullable String getLabel() { return label; }
    public boolean hasLabel() { return labelPresent; }

    public @Nullable CompletableFuture<WorkResult> next() throws Exception {
        if (isCancelled()) return null;
        if (!iterator.hasNext()) { close(); return null; }
        Object value = iterator.next();
        IncrementalWork work = prepareItem();
        if (work == null) return null;
        CompletableFuture<WorkResult> result = completeItem.complete(value, index++, work);
        result.whenComplete((completed, exception) -> retain(work, completed));
        return result;
    }

    private synchronized boolean isCancelled() { return cancelled; }

    private synchronized @Nullable IncrementalWork prepareItem() {
        if (cancelled) return null;
        pendingWork = producerWork.newChild();
        return pendingWork;
    }

    private void retain(IncrementalWork work, @Nullable WorkResult result) {
        if (result != null && noLongerOwned(work)) result.getWork().dispose();
    }

    private synchronized boolean noLongerOwned(IncrementalWork work) {
        return cancelled || pendingWork != work;
    }

    public synchronized void consumed() { pendingWork = null; }

    public void cancel() {
        IncrementalWork work;
        synchronized (this) {
            if (cancelled) return;
            cancelled = true;
            work = pendingWork;
        }
        if (work != null) work.dispose();
        try { close(); } catch (Exception ignored) { }
    }

    private void close() throws Exception {
        if (!closed.compareAndSet(false, true)) return;
        if (resource != null) resource.close();
    }
}
