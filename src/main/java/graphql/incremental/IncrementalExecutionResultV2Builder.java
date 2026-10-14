package graphql.incremental;

import graphql.ExecutionResultImpl;
import graphql.Internal;
import org.jspecify.annotations.NullUnmarked;
import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;

@Internal
@NullUnmarked
public final class IncrementalExecutionResultV2Builder extends ExecutionResultImpl.Builder<IncrementalExecutionResultV2Builder> {
    private List<PendingPayload> pending = Collections.emptyList();
    private Publisher<DelayedIncrementalPartialResult> publisher;

    public IncrementalExecutionResultV2Builder pending(List<PendingPayload> pending) {
        this.pending = pending;
        return this;
    }

    public IncrementalExecutionResultV2Builder incrementalItemPublisher(Publisher<DelayedIncrementalPartialResult> publisher) {
        this.publisher = publisher;
        return this;
    }

    @Override
    public IncrementalExecutionResultV2 build() {
        return new IncrementalExecutionResultV2(this, pending, publisher);
    }
}
