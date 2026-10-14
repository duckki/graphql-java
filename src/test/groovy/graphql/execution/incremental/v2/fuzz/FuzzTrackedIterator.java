package graphql.execution.incremental.v2.fuzz;

import graphql.Internal;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

@Internal
public final class FuzzTrackedIterator implements Iterator<Object> {
    private final Iterator<?> delegate;
    private final AtomicInteger pulls;

    public FuzzTrackedIterator(Iterator<?> delegate, AtomicInteger pulls) { this.delegate = delegate; this.pulls = pulls; }
    @Override
    public boolean hasNext() { return delegate.hasNext(); }
    @Override
    public Object next() { pulls.incrementAndGet(); return delegate.next(); }
}
