package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.incremental.DelayedIncrementalPartialResult;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNullWithNPE;

@Internal
public final class IncrementalResultPublisher implements Publisher<DelayedIncrementalPartialResult> {
    private final IncrementalWorkQueue queue;
    private Consumer<Throwable> onFinished;
    private Throwable terminalError;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();
    private Subscriber<? super DelayedIncrementalPartialResult> subscriber;
    private long demand;
    private boolean pumping;
    private volatile boolean closed;

    public IncrementalResultPublisher(IncrementalWorkQueue queue, Runnable onFinished) {
        this.queue = queue;
        this.onFinished = exception -> onFinished.run();
    }

    public void onFinished(Consumer<Throwable> callback) {
        this.onFinished = callback;
    }

    @Override
    public void subscribe(Subscriber<? super DelayedIncrementalPartialResult> subscriber) {
        assertNotNullWithNPE(subscriber, "Subscriber must not be null");
        if (!subscribed.compareAndSet(false, true)) {
            subscriber.onSubscribe(new RejectedSubscription());
            subscriber.onError(new IllegalStateException("This publisher only supports one subscriber"));
            return;
        }
        queue.dispatch(() -> attach(subscriber));
    }

    private void attach(Subscriber<? super DelayedIncrementalPartialResult> subscriber) {
        this.subscriber = subscriber;
        subscriber.onSubscribe(new IncrementalSubscription(this));
        if (terminalError != null) {
            subscriber.onError(terminalError);
            this.subscriber = null;
            return;
        }
        if (!closed) queue.start();
    }

    public void request(long count) {
        if (count <= 0) {
            abort(new IllegalArgumentException("Reactive streams rule 3.9: demand must be positive"));
            return;
        }
        queue.dispatch(() -> addDemand(count));
    }

    private void addDemand(long count) {
        if (closed) return;
        demand = demand > Long.MAX_VALUE - count ? Long.MAX_VALUE : demand + count;
        pump();
    }

    public void pump() {
        if (pumping) return;
        pumping = true;
        try { emitAvailable(); }
        finally { pumping = false; }
    }

    private void emitAvailable() {
        while (demand > 0 && !closed) {
            DelayedIncrementalPartialResult result = queue.nextBatch();
            if (result == null) return;
            demand--;
            subscriber.onNext(result);
            if (!result.hasNext() && !closed) complete();
        }
    }

    private void complete() {
        closed = true;
        Subscriber<? super DelayedIncrementalPartialResult> target = subscriber;
        subscriber = null;
        finish(null);
        target.onComplete();
    }

    public void cancel() {
        if (closed) return;
        closed = true;
        queue.cancel();
        queue.dispatch(() -> { subscriber = null; finish(null); });
    }

    public void abort(Throwable exception) {
        queue.cancel();
        queue.dispatch(() -> fail(exception));
    }

    public void fail(Throwable exception) {
        if (closed) return;
        closed = true;
        terminalError = exception;
        queue.cancel();
        Subscriber<? super DelayedIncrementalPartialResult> target = subscriber;
        subscriber = null;
        queue.dispatch(() -> { finish(exception); if (target != null) target.onError(exception); });
    }

    private void finish(Throwable exception) {
        if (!finished.compareAndSet(false, true)) return;
        Consumer<Throwable> callback = onFinished;
        onFinished = null;
        callback.accept(exception);
    }
}
