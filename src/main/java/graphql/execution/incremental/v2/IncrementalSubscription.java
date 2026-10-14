package graphql.execution.incremental.v2;

import graphql.Internal;
import org.reactivestreams.Subscription;

@Internal
public final class IncrementalSubscription implements Subscription {
    private final IncrementalResultPublisher publisher;

    public IncrementalSubscription(IncrementalResultPublisher publisher) {
        this.publisher = publisher;
    }
    @Override
    public void request(long count) {
        publisher.request(count);
    }
    @Override
    public void cancel() {
        publisher.cancel();
    }
}
