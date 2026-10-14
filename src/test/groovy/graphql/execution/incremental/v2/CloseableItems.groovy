package graphql.execution.incremental.v2

import java.util.concurrent.atomic.AtomicInteger

class CloseableItems implements Iterable<Object>, AutoCloseable {
    List<Object> values
    final AtomicInteger pulled = new AtomicInteger()
    final AtomicInteger closed = new AtomicInteger()

    Iterator<Object> iterator() {
        def delegate = values.iterator()
        return [hasNext: { delegate.hasNext() }, next: { pulled.incrementAndGet(); delegate.next() }] as Iterator<Object>
    }

    void close() { closed.incrementAndGet() }
}
