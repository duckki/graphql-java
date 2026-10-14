package graphql.execution.incremental.v2.fuzz;

import graphql.Internal;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Internal
public final class FuzzTrackedIterable implements Iterable<Object>, AutoCloseable {
    private final List<?> values;
    public final List<Object> path;
    public final int initialCount;
    public final AtomicInteger acquired = new AtomicInteger();
    public final AtomicInteger pulls = new AtomicInteger();
    public final AtomicInteger closes = new AtomicInteger();

    public FuzzTrackedIterable(List<?> values, List<Object> path, int initialCount) {
        this.values = values;
        this.path = List.copyOf(path);
        this.initialCount = initialCount;
    }

    @Override
    public Iterator<Object> iterator() {
        acquired.incrementAndGet();
        Iterator<Object> iterator = new FuzzTrackedIterator(values.iterator(), pulls);
        return closesIterator() ? new FuzzCloseableIterator(iterator, closes) : iterator;
    }

    public boolean closesIterator() { return (path.hashCode() & 1) == 0; }
    public int size() { return values.size(); }

    @Override
    public void close() { closes.incrementAndGet(); }
}
