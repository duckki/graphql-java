package graphql.execution.incremental.v2.fuzz;

import graphql.Internal;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

@Internal
public final class FuzzCloseableIterator implements Iterator<Object>, AutoCloseable {
    private final Iterator<Object> delegate;
    private final AtomicInteger closes;

    public FuzzCloseableIterator(Iterator<Object> delegate, AtomicInteger closes) { this.delegate = delegate; this.closes = closes; }
    @Override
    public boolean hasNext() { return delegate.hasNext(); }
    @Override
    public Object next() { return delegate.next(); }
    @Override
    public void close() { closes.incrementAndGet(); }
}
