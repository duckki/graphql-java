package graphql.execution.incremental.v2;

import graphql.GraphQLError;
import graphql.Internal;
import graphql.execution.ExecutionStrategyParameters;
import graphql.execution.ResultPath;
import graphql.execution.incremental.AlternativeCallContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static graphql.Assert.assertNotNull;

@Internal
public final class IncrementalErrorSupport {
    private IncrementalErrorSupport() { }

    public static void add(ExecutionStrategyParameters parameters, GraphQLError error, ResultPath path) {
        AlternativeCallContext alternative = assertNotNull(parameters.getAlternativeCallContext());
        if (parameters.getIncrementalExecutionParameters() == null) {
            alternative.addError(error);
            return;
        }
        List<GraphQLError> errors = alternative.getErrors();
        synchronized (errors) {
            if (errors.stream().anyMatch(existing -> path.toList().equals(existing.getPath()))) return;
            alternative.addError(error);
        }
    }

    public static boolean propagate(ExecutionStrategyParameters parameters, CompletableFuture<?> result, Throwable exception) {
        AlternativeCallContext alternative = parameters.getAlternativeCallContext();
        if (parameters.getIncrementalExecutionParameters() == null || alternative == null
                || parameters.getPath().getLevel() != alternative.getStartLevel() - 1) return false;
        result.completeExceptionally(exception);
        return true;
    }
}
