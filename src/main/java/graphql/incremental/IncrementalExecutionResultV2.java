package graphql.incremental;

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.Internal;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Internal
@NullMarked
public final class IncrementalExecutionResultV2 extends ExecutionResultImpl implements IncrementalExecutionResult {
    private final List<PendingPayload> pending;
    private final Publisher<DelayedIncrementalPartialResult> publisher;

    public IncrementalExecutionResultV2(IncrementalExecutionResultV2Builder builder, List<PendingPayload> pending,
                                        Publisher<DelayedIncrementalPartialResult> publisher) {
        super(builder);
        this.pending = List.copyOf(pending);
        this.publisher = graphql.Assert.assertNotNull(publisher);
    }

    @Override
    public List<PendingPayload> getPending() {
        return pending;
    }

    @Override
    public boolean hasNext() {
        return !pending.isEmpty();
    }

    @Override
    public @Nullable List<IncrementalPayload> getIncremental() {
        return null;
    }

    @Override
    public Publisher<DelayedIncrementalPartialResult> getIncrementalItemPublisher() {
        return publisher;
    }

    @Override
    public Map<String, Object> toSpecification() {
        Map<String, Object> result = new LinkedHashMap<>(super.toSpecification());
        result.put("hasNext", hasNext());
        if (!pending.isEmpty()) result.put("pending", pending.stream().map(PendingPayload::toSpecification).collect(Collectors.toList()));
        return result;
    }

    public static IncrementalExecutionResultV2Builder newIncrementalExecutionResultV2() {
        return new IncrementalExecutionResultV2Builder();
    }

    @Override
    public IncrementalExecutionResultV2 transform(Consumer<ExecutionResult.Builder<?>> consumer) {
        IncrementalExecutionResultV2Builder builder = newIncrementalExecutionResultV2().from(this).pending(pending).incrementalItemPublisher(publisher);
        consumer.accept(builder);
        return builder.build();
    }
}
