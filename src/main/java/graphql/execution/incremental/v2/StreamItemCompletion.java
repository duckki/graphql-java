package graphql.execution.incremental.v2;

import graphql.Internal;

import java.util.concurrent.CompletableFuture;

@Internal
@FunctionalInterface
public interface StreamItemCompletion {
    CompletableFuture<WorkResult> complete(Object value, int index, IncrementalWork work);
}
