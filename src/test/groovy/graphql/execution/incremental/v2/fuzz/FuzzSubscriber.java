package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import graphql.Internal;
import graphql.incremental.DelayedIncrementalPartialResult;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletableFuture;

@Internal
public final class FuzzSubscriber implements Subscriber<DelayedIncrementalPartialResult> {
    private final ObjectMapper json = new ObjectMapper();
    public final ArrayNode updates = json.createArrayNode();
    public final CompletableFuture<Void> finished = new CompletableFuture<>();
    private Subscription subscription;
    private final long batch;
    private long remaining;

    public FuzzSubscriber() { this("one"); }
    public FuzzSubscriber(String demand) {
        batch = demand.equals("all") ? Long.MAX_VALUE : demand.equals("burst") ? 3 : 1;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        remaining = batch;
        subscription.request(batch);
        if (batch == Long.MAX_VALUE) subscription.request(batch);
    }
    @Override
    public void onNext(DelayedIncrementalPartialResult result) {
        updates.add(json.valueToTree(result.toSpecification()));
        if (updates.size() > 4096) { subscription.cancel(); finished.completeExceptionally(new IllegalStateException("Too many incremental updates")); return; }
        if (batch != Long.MAX_VALUE && --remaining == 0) { remaining = batch; subscription.request(batch); }
    }
    @Override
    public void onError(Throwable exception) { finished.completeExceptionally(exception); }
    @Override
    public void onComplete() { finished.complete(null); }
    public void cancel() { if (subscription != null) subscription.cancel(); }
}
