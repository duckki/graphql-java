package graphql.execution.incremental.v2;

import graphql.ExecutionResult;
import graphql.ExperimentalApi;
import graphql.Internal;
import graphql.execution.AbortExecutionException;
import graphql.execution.ExecutionContext;
import graphql.execution.FieldCollectorParameters;
import graphql.execution.ResultPath;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationReactiveResultsParameters;
import graphql.incremental.IncrementalExecutionResultV2;
import graphql.incremental.PendingPayload;
import graphql.language.OperationDefinition;
import org.jspecify.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static graphql.execution.instrumentation.SimpleInstrumentationContext.nonNullCtx;

@Internal
public final class IncrementalExecution {
    public @Nullable IncrementalExecutionParameters createParameters(ExecutionContext context, FieldCollectorParameters collector,
                                                                      OperationDefinition operation, ResultPath path) {
        if (!context.getGraphQLContext().getBoolean(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2)
                || operation.getOperation() == OperationDefinition.Operation.SUBSCRIPTION) return null;
        CollectedFields fields = new IncrementalFieldCollector().collectFields(collector, operation.getSelectionSet());
        IncrementalWork work = new IncrementalWork();
        context.getExecutionInput().getCancellationFuture().thenRun(() -> {
            work.getControl().stop();
            work.dispose();
        });
        return new IncrementalExecutionParameters(work, fields, Collections.emptySet(), new DeferContext(null, Collections.emptyMap()))
                .withFields(fields, path);
    }

    public CompletableFuture<ExecutionResult> complete(ExecutionContext context, CompletableFuture<ExecutionResult> result, IncrementalExecutionParameters parameters) {
        parameters.declareGroups();
        IncrementalWork work = parameters.getWork();
        return result.handle((value, exception) -> initialResult(context, work, value, exception))
                .thenApply(initial -> buildResponse(context, initial, work));
    }

    private ExecutionResult initialResult(ExecutionContext context, IncrementalWork work, ExecutionResult value, Throwable exception) {
        if (exception == null) return value;
        work.dispose();
        Throwable cause = exception;
        while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
        if (cause instanceof CancellationException && context.getExecutionInput().isCancelled()) {
            return new AbortExecutionException("Incremental execution was cancelled").toExecutionResult();
        }
        throw new CompletionException(exception);
    }

    private ExecutionResult buildResponse(ExecutionContext context, ExecutionResult initial, IncrementalWork work) {
        work.filter(initial.getData(), ResultPath.rootPath());
        IncrementalWorkQueue queue = new IncrementalWorkQueue(work, () -> { });
        List<PendingPayload> pending = queue.getInitialPending();
        if (pending.isEmpty()) return initial;
        InstrumentationReactiveResultsParameters parameters = new InstrumentationReactiveResultsParameters(context,
                InstrumentationReactiveResultsParameters.ResultType.DEFER);
        InstrumentationContext<Void> instrumentation = nonNullCtx(context.getInstrumentation().beginReactiveResults(parameters,
                context.getInstrumentationState()));
        queue.getPublisher().onFinished(exception -> instrumentation.onCompleted(null, exception));
        instrumentation.onDispatched();
        registerCancellation(context, queue);
        return IncrementalExecutionResultV2.newIncrementalExecutionResultV2().from(initial).pending(pending)
                .incrementalItemPublisher(queue.getPublisher()).build();
    }

    private void registerCancellation(ExecutionContext context, IncrementalWorkQueue queue) {
        WeakReference<IncrementalResultPublisher> reference = new WeakReference<>(queue.getPublisher());
        context.getExecutionInput().getCancellationFuture().thenRun(() -> {
            IncrementalResultPublisher publisher = reference.get();
            if (publisher == null) return;
            publisher.abort(new AbortExecutionException("Incremental execution was cancelled"));
        });
    }
}
