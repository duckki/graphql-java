package graphql.incremental;

import graphql.Internal;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Internal
@NullMarked
public final class DelayedIncrementalPartialResultV2 implements DelayedIncrementalPartialResult {
    private final List<PendingPayload> pending;
    private final List<IncrementalPayload> incremental;
    private final List<CompletedPayload> completed;
    private final boolean hasNext;

    public DelayedIncrementalPartialResultV2(List<PendingPayload> pending, List<IncrementalPayload> incremental,
                                             List<CompletedPayload> completed, boolean hasNext) {
        this.pending = List.copyOf(pending);
        this.incremental = List.copyOf(incremental);
        this.completed = List.copyOf(completed);
        this.hasNext = hasNext;
    }

    @Override
    public List<PendingPayload> getPending() {
        return pending;
    }

    @Override
    public List<IncrementalPayload> getIncremental() {
        return incremental;
    }

    @Override
    public List<CompletedPayload> getCompleted() {
        return completed;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public @Nullable Map<Object, Object> getExtensions() {
        return null;
    }

    @Override
    public Map<String, Object> toSpecification() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("hasNext", hasNext);
        if (!pending.isEmpty()) result.put("pending", pending.stream().map(PendingPayload::toSpecification).collect(Collectors.toList()));
        if (!incremental.isEmpty()) result.put("incremental", incremental.stream().map(IncrementalPayload::toSpecification).collect(Collectors.toList()));
        if (!completed.isEmpty()) result.put("completed", completed.stream().map(CompletedPayload::toSpecification).collect(Collectors.toList()));
        return result;
    }

    public static DelayedIncrementalPartialResultV2Builder newDelayedIncrementalPartialResultV2() {
        return new DelayedIncrementalPartialResultV2Builder();
    }

    public DelayedIncrementalPartialResultV2 transform(Consumer<DelayedIncrementalPartialResultV2Builder> consumer) {
        DelayedIncrementalPartialResultV2Builder builder = newDelayedIncrementalPartialResultV2()
                .pending(pending).incrementalItems(incremental).completed(completed).hasNext(hasNext);
        consumer.accept(builder);
        return builder.build();
    }
}
