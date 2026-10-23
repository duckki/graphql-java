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
    private final @Nullable List<GraphQLError> cause;

    public WorkResult(@Nullable Object value, List<GraphQLError> errors, IncrementalWork work, boolean failed) {
        this(value, errors, work, failed, null);
    }

    public WorkResult(@Nullable Object value, List<GraphQLError> errors, IncrementalWork work, boolean failed, @Nullable List<GraphQLError> cause) {
        this.value = value;
        this.errors = List.copyOf(errors);
        this.work = graphql.Assert.assertNotNull(work);
        this.failed = failed;
        this.cause = cause == null ? null : List.copyOf(cause);
    }

    public @Nullable Object getValue() {
        return value;
    }
    public List<GraphQLError> getErrors() {
        return errors;
    }
    public List<GraphQLError> getDeliveryErrors() {
        return failed && cause != null ? cause : errors;
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
        WorkResultBuilder builder = newWorkResult().value(value).errors(errors).work(work).failed(failed).cause(cause);
        consumer.accept(builder);
        return builder.build();
    }
}
