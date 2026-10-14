package graphql.execution.incremental.v2.fuzz;

import graphql.Internal;
import graphql.ExecutionInput;
import graphql.incremental.DelayedIncrementalPartialResult;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

@Internal
public final class FuzzHostSubscriber implements Subscriber<DelayedIncrementalPartialResult> {
    public final List<Map<String, Object>> updates = new CopyOnWriteArrayList<>();
    public final CompletableFuture<Void> finished = new CompletableFuture<>();
    public Throwable error;
    private final ExecutionInput input;
    private final int policy;
    private Subscription subscription;

    public FuzzHostSubscriber(ExecutionInput input, int policy) { this.input = input; this.policy = policy; }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        if (policy == 0) { subscription.cancel(); finished.complete(null); return; }
        if (policy == 3) { subscription.request(0); return; }
        if (policy == 4) { input.cancel(); return; }
        subscription.request(1);
    }

    @Override
    public void onNext(DelayedIncrementalPartialResult result) {
        updates.add(result.toSpecification());
        if (updates.size() >= (policy == 2 ? 2 : 1)) { subscription.cancel(); finished.complete(null); return; }
        subscription.request(1);
    }

    @Override
    public void onError(Throwable exception) { error = exception; finished.complete(null); }
    @Override
    public void onComplete() { finished.complete(null); }
}
