package graphql.execution.incremental.v2;

import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.Internal;
import graphql.execution.NonNullableFieldWasNullException;
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

    public static List<GraphQLError> deliveryCause(Throwable exception, List<GraphQLError> errors, ResultPath path) {
        Throwable current = exception;
        ResultPath origin = null;
        while (current != null) {
            if (current instanceof NonNullableFieldWasNullException) origin = ((NonNullableFieldWasNullException) current).getPath();
            current = current.getCause();
        }
        if (origin == null) return from(exception, path);
        List<Object> segments = origin.toList();
        for (GraphQLError error : errors) {
            if (segments.equals(error.getPath())) return Collections.singletonList(error);
        }
        return from(exception, origin);
    }
}
