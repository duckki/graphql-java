package graphql.execution.incremental.v2.fuzz;

import graphql.Internal;

import java.util.concurrent.ConcurrentLinkedQueue;

@Internal
public final class FuzzHostState {
    public final ConcurrentLinkedQueue<FuzzTrackedIterable> sources = new ConcurrentLinkedQueue<>();
}
