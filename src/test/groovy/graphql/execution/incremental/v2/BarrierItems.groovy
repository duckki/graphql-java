package graphql.execution.incremental.v2

import java.util.concurrent.CompletableFuture

class BarrierItems extends CloseableItems {
    final CompletableFuture<Void> acquired = new CompletableFuture<>()
    final CompletableFuture<Void> firstPull = new CompletableFuture<>()

    Iterator<Object> iterator() {
        def delegate = super.iterator()
        acquired.complete(null)
        [hasNext: { delegate.hasNext() }, next: {
            def value = delegate.next()
            firstPull.complete(null)
            value
        }] as Iterator<Object>
    }
}
