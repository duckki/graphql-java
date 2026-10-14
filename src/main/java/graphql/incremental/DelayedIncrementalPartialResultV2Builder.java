package graphql.incremental;

import graphql.Internal;
import org.jspecify.annotations.NullUnmarked;

import java.util.Collections;
import java.util.List;

@Internal
@NullUnmarked
public final class DelayedIncrementalPartialResultV2Builder {
    private List<PendingPayload> pending = Collections.emptyList();
    private List<IncrementalPayload> incremental = Collections.emptyList();
    private List<CompletedPayload> completed = Collections.emptyList();
    private boolean hasNext;

    public DelayedIncrementalPartialResultV2Builder pending(List<PendingPayload> pending) {
        this.pending = pending;
        return this;
    }

    public DelayedIncrementalPartialResultV2Builder incrementalItems(List<IncrementalPayload> incremental) {
        this.incremental = incremental;
        return this;
    }

    public DelayedIncrementalPartialResultV2Builder completed(List<CompletedPayload> completed) {
        this.completed = completed;
        return this;
    }

    public DelayedIncrementalPartialResultV2Builder hasNext(boolean hasNext) {
        this.hasNext = hasNext;
        return this;
    }

    public DelayedIncrementalPartialResultV2 build() {
        return new DelayedIncrementalPartialResultV2(pending, incremental, completed, hasNext);
    }
}
