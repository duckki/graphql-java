package graphql.execution.incremental.v2;

import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.ResultPath;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Internal
public final class DeferredTask {
    private final List<DeliveryGroup> groups;
    private final ResultPath path;
    private final IncrementalWork work;
    private Supplier<CompletableFuture<WorkResult>> computation;
    private CompletableFuture<WorkResult> result;
    private boolean aborted;

    public DeferredTask(List<DeliveryGroup> groups, ResultPath path, IncrementalWork work, Supplier<CompletableFuture<WorkResult>> computation) {
        this.groups = List.copyOf(groups);
        this.path = path;
        this.work = work;
        this.computation = computation;
    }

    public List<DeliveryGroup> getGroups() {
        return groups;
    }
    public ResultPath getPath() {
        return path;
    }

    public synchronized CompletableFuture<WorkResult> result() {
        if (result != null) return result;
        if (aborted) return CompletableFuture.failedFuture(new java.util.concurrent.CancellationException());
        Supplier<CompletableFuture<WorkResult>> execute = computation;
        computation = null;
        try { result = execute.get(); }
        catch (Exception exception) { result = CompletableFuture.failedFuture(exception); }
        return result;
    }

    public synchronized @Nullable List<GraphQLError> getRejection() {
        if (result == null || !result.isDone()) return null;
        try {
            WorkResult value = result.getNow(null);
            return value != null && value.isFailed() ? value.getDeliveryErrors() : null;
        } catch (Exception exception) {
            return IncrementalErrors.from(exception, path);
        }
    }

    public synchronized void abort() {
        if (aborted || getRejection() != null) return;
        aborted = true;
        computation = null;
        work.dispose();
    }
}
