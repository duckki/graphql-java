package graphql.execution.incremental.v2;

import graphql.Internal;
import org.reactivestreams.Subscription;

@Internal
public final class RejectedSubscription implements Subscription {
    @Override
    public void request(long count) { }
    @Override
    public void cancel() { }
}
