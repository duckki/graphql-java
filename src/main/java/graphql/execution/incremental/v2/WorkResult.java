package graphql.execution.incremental.v2;

import graphql.GraphQLError;
import graphql.Internal;
import org.jspecify.annotations.Nullable;

import java.util.List;

@Internal
public final class WorkResult {
    private final @Nullable Object value;
    private final List<GraphQLError> errors;
    private final IncrementalWork work;
    private final boolean failed;

    public WorkResult(@Nullable Object value, List<GraphQLError> errors, IncrementalWork work, boolean failed) {
        this.value = value;
        this.errors = List.copyOf(errors);
        this.work = graphql.Assert.assertNotNull(work);
        this.failed = failed;
    }

    public @Nullable Object getValue() {
        return value;
    }
    public List<GraphQLError> getErrors() {
        return errors;
    }
    public IncrementalWork getWork() {
        return work;
    }
    public boolean isFailed() {
        return failed;
    }

    public static WorkResultBuilder newWorkResult() {
        return new WorkResultBuilder();
    }

    public WorkResult transform(java.util.function.Consumer<WorkResultBuilder> consumer) {
        WorkResultBuilder builder = newWorkResult().value(value).errors(errors).work(work).failed(failed);
        consumer.accept(builder);
        return builder.build();
    }
}
