package graphql.execution.incremental.v2

import graphql.incremental.DelayedIncrementalPartialResult
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

class IncrementalTestSubscriber implements Subscriber<DelayedIncrementalPartialResult> {
    final List<Map> results = new CopyOnWriteArrayList<>()
    final CompletableFuture<Void> finished = new CompletableFuture<>()
    Subscription subscription
    long initialDemand = Long.MAX_VALUE
    Closure onResult = {}

    void onSubscribe(Subscription subscription) {
        this.subscription = subscription
        if (initialDemand > 0) subscription.request(initialDemand)
    }

    void onNext(DelayedIncrementalPartialResult result) {
        results.add(result.toSpecification())
        onResult(result)
    }

    void onError(Throwable exception) { finished.completeExceptionally(exception) }
    void onComplete() { finished.complete(null) }
    List<Map> awaitResults() { finished.get(5, TimeUnit.SECONDS); results }
}
