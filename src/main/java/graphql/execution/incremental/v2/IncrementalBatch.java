package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.incremental.CompletedPayload;
import graphql.incremental.DelayedIncrementalPartialResult;
import graphql.incremental.DelayedIncrementalPartialResultV2;
import graphql.incremental.IncrementalPayload;
import graphql.incremental.PendingPayload;

import java.util.ArrayList;
import java.util.List;

@Internal
public final class IncrementalBatch {
    public final List<PendingPayload> pending = new ArrayList<>();
    public final List<IncrementalPayload> incremental = new ArrayList<>();
    public final List<CompletedPayload> completed = new ArrayList<>();

    public boolean isEmpty() {
        return pending.isEmpty() && incremental.isEmpty() && completed.isEmpty();
    }

    public DelayedIncrementalPartialResult toResult(boolean hasNext) {
        return DelayedIncrementalPartialResultV2.newDelayedIncrementalPartialResultV2()
                .pending(pending).incrementalItems(incremental)
                .completed(completed).hasNext(hasNext).build();
    }
}
