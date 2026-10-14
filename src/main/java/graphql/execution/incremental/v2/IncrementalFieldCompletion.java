package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.Async;
import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ExecutionStrategyParameters;
import graphql.execution.incremental.AlternativeCallContext;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationFieldParameters;

import java.util.function.Function;
import java.util.function.Supplier;

import static graphql.execution.instrumentation.SimpleInstrumentationContext.nonNullCtx;

@Internal
public final class IncrementalFieldCompletion {
    public Object resolve(ExecutionContext context, ExecutionStrategyParameters parameters, Supplier<ExecutionStepInfo> step,
                           Function<ExecutionStrategyParameters, Object> resolveField) {
        AlternativeCallContext alternative = parameters.getAlternativeCallContext();
        if (alternative == null || parameters.getPath().getLevel() != alternative.getStartLevel()) return resolveField.apply(parameters);
        InstrumentationFieldParameters fieldParameters = new InstrumentationFieldParameters(context, step);
        InstrumentationContext<Object> instrumentation = nonNullCtx(context.getInstrumentation()
                .beginDeferredField(fieldParameters, context.getInstrumentationState()));
        Object result = resolveField.apply(parameters);
        context.getDataLoaderDispatcherStrategy().deferFieldFetched(parameters);
        instrumentation.onDispatched();
        Async.toCompletableFuture(result).whenComplete(instrumentation::onCompleted);
        return result;
    }
}
