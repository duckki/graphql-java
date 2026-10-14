package graphql.execution.incremental.v2;

import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.Internal;
import graphql.execution.ResultPath;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;

@Internal
public final class IncrementalErrors {
    private IncrementalErrors() { }

    public static List<GraphQLError> from(Throwable exception, ResultPath path) {
        Throwable cause = exception instanceof CompletionException && exception.getCause() != null ? exception.getCause() : exception;
        if (cause instanceof GraphQLError) return Collections.singletonList((GraphQLError) cause);
        return Collections.singletonList(GraphqlErrorBuilder.newError()
                .message("Incremental execution failed: %s", cause.getMessage()).path(path.toList()).build());
    }
}
