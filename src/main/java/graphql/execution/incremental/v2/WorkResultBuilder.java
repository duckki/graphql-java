package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.GraphQLError;
import org.jspecify.annotations.NullUnmarked;

import java.util.Collections;
import java.util.List;

@Internal
@NullUnmarked
public final class WorkResultBuilder {
    private Object value = null;
    private List<GraphQLError> errors = Collections.emptyList();
    private IncrementalWork work = null;
    private boolean failed = false;

    public WorkResultBuilder value(Object value) {
        this.value = value;
        return this;
    }

    public WorkResultBuilder errors(List<GraphQLError> errors) {
        this.errors = errors;
        return this;
    }

    public WorkResultBuilder work(IncrementalWork work) {
        this.work = work;
        return this;
    }

    public WorkResultBuilder failed(boolean failed) {
        this.failed = failed;
        return this;
    }

    public WorkResult build() {
        return new WorkResult(value, errors, work, failed);
    }
}
