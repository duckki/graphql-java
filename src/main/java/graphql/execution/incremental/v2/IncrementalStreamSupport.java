package graphql.execution.incremental.v2;

import graphql.Directives;
import graphql.Internal;
import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.execution.Async;
import graphql.execution.DataFetcherResult;
import graphql.execution.NonNullableFieldWasNullException;
import graphql.execution.CoercedVariables;
import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ExecutionStepInfoFactory;
import graphql.execution.ExecutionStrategyParameters;
import graphql.execution.FieldValueInfo;
import graphql.execution.MergedField;
import graphql.execution.ResultPath;
import graphql.execution.ValuesResolver;
import graphql.execution.incremental.AlternativeCallContext;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.parameters.InstrumentationFieldCompleteParameters;
import graphql.language.Directive;
import graphql.language.NodeUtil;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static graphql.Assert.assertNotNull;
import static graphql.execution.instrumentation.SimpleInstrumentationContext.nonNullCtx;

@Internal
public final class IncrementalStreamSupport {
    public @Nullable FieldValueInfo complete(ExecutionContext context, ExecutionStrategyParameters parameters, Iterable<Object> values,
                                             Function<ExecutionStrategyParameters, FieldValueInfo> completeItem) {
        if (parameters.getPath().isListSegment()) return null;
        MergedField field = assertNotNull(parameters.getField());
        Directive directive = NodeUtil.findNodeByName(field.getSingleField().getDirectives(), "stream");
        if (directive == null) return null;
        Map<String, Object> arguments = ValuesResolver.getArgumentValues(Directives.StreamDirective.getArguments(), directive.getArguments(),
                CoercedVariables.of(context.getCoercedVariables().toMap()), context.getGraphQLContext(), context.getLocale());
        if (Boolean.FALSE.equals(arguments.get("if"))) return null;
        int initialCount = (Integer) arguments.get("initialCount");
        if (initialCount < 0) return invalidCount(context, parameters, directive);
        return completeStream(context, parameters, values, initialCount, (String) arguments.get("label"), directive.getArgument("label") != null, completeItem);
    }

    private FieldValueInfo invalidCount(ExecutionContext context, ExecutionStrategyParameters parameters, Directive directive) {
        GraphQLError error = GraphqlErrorBuilder.newError().message("The @stream initialCount argument must be non-negative")
                .path(parameters.getPath().toList()).location(directive.getSourceLocation()).build();
        AlternativeCallContext alternative = parameters.getAlternativeCallContext();
        if (alternative == null) context.addError(error, parameters.getPath());
        else alternative.addError(error);
        try {
            return new FieldValueInfo(FieldValueInfo.CompleteValueType.LIST, parameters.getNonNullFieldValidator().validate(parameters, null));
        } catch (NonNullableFieldWasNullException exception) {
            return new FieldValueInfo(FieldValueInfo.CompleteValueType.LIST, CompletableFuture.failedFuture(exception));
        }
    }

    private CompletableFuture<Object> resolvedItem(Object raw, ResultPath path) {
        return Async.toCompletableFuture(raw).handle((value, exception) -> exception == null ? value
                : DataFetcherResult.newResult().errors(IncrementalErrors.from(exception, path)).build());
    }

    private FieldValueInfo completeStream(ExecutionContext context, ExecutionStrategyParameters parameters, Iterable<Object> values,
                                           int initialCount, @Nullable String label, boolean labelPresent, Function<ExecutionStrategyParameters, FieldValueInfo> completeItem) {
        InstrumentationFieldCompleteParameters instrumentationParameters = new InstrumentationFieldCompleteParameters(context, parameters,
                parameters::getExecutionStepInfo, values);
        InstrumentationContext<Object> instrumentation = nonNullCtx(context.getInstrumentation().beginFieldListCompletion(
                instrumentationParameters, context.getInstrumentationState()));
        Iterator<Object> iterator = values.iterator();
        AutoCloseable resource = iterator instanceof AutoCloseable ? (AutoCloseable) iterator : values instanceof AutoCloseable ? (AutoCloseable) values : null;
        List<FieldValueInfo> infos = new ArrayList<>();
        int index = 0;
        try {
            while (index < initialCount && iterator.hasNext()) {
                infos.add(completePrefix(parameters, iterator.next(), index++, completeItem));
            }
            registerTail(context, parameters, iterator, index, label, labelPresent, resource, completeItem);
        } catch (Exception exception) {
            close(resource);
            instrumentation.onCompleted(null, exception);
            return new FieldValueInfo(FieldValueInfo.CompleteValueType.LIST, CompletableFuture.failedFuture(exception), infos);
        }
        Object results = Async.eachPolymorphic(infos, FieldValueInfo::getFieldValueObject);
        instrumentation.onDispatched();
        Async.toCompletableFuture(results).whenComplete(instrumentation::onCompleted);
        return new FieldValueInfo(FieldValueInfo.CompleteValueType.LIST, results, infos);
    }

    private FieldValueInfo completePrefix(ExecutionStrategyParameters parameters, Object raw, int index,
                                           Function<ExecutionStrategyParameters, FieldValueInfo> completeItem) {
        CompletableFuture<Object> result = resolvedItem(raw, parameters.getPath().segment(index)).thenCompose(value ->
                completeItem.apply(itemParameters(parameters, value, index)).getFieldValueFuture());
        return new FieldValueInfo(FieldValueInfo.CompleteValueType.OBJECT, result);
    }

    private void registerTail(ExecutionContext context, ExecutionStrategyParameters parameters, Iterator<Object> iterator, int index,
                               @Nullable String label, boolean labelPresent, @Nullable AutoCloseable resource,
                               Function<ExecutionStrategyParameters, FieldValueInfo> completeItem) throws Exception {
        IncrementalExecutionParameters incremental = assertNotNull(parameters.getIncrementalExecutionParameters());
        if (!iterator.hasNext()) { if (resource != null) resource.close(); return; }
        ItemStream stream = new ItemStream(parameters.getPath(), label, labelPresent, iterator, index, resource, incremental.getWork(),
                (raw, itemIndex, work) -> completeRemaining(context, parameters, raw, itemIndex, work, completeItem));
        incremental.getWork().addStream(stream);
    }

    private CompletableFuture<WorkResult> completeRemaining(ExecutionContext context, ExecutionStrategyParameters parameters,
                                                            Object raw, int index, IncrementalWork work, Function<ExecutionStrategyParameters, FieldValueInfo> completeItem) {
        AlternativeCallContext errors = new StreamCallContext(parameters.getPath().segment(index));
        CollectedFields fields = new CollectedFields();
        IncrementalExecutionParameters inherited = assertNotNull(parameters.getIncrementalExecutionParameters());
        List<FieldDetails> details = inherited.getCollectedFields().getFields().get(assertNotNull(parameters.getField()).getResultKey());
        details.forEach(detail -> fields.addField(new FieldDetails(detail.getField(), null)));
        IncrementalExecutionParameters incremental = new IncrementalExecutionParameters(work, fields, Collections.emptySet(), new DeferContext(null, Collections.emptyMap()));
        ExecutionStrategyParameters item = parameters.transform(builder -> builder.alternativeCallContext(errors).incrementalExecutionParameters(incremental));
        return resolvedItem(raw, parameters.getPath().segment(index))
                .thenCompose(value -> completeItem.apply(itemParameters(item, value, index)).getFieldValueFuture())
                .handle((value, exception) -> finishItem(context, item.getPath().segment(index), work, errors, value, exception));
    }

    private WorkResult finishItem(ExecutionContext context, ResultPath path, IncrementalWork work, AlternativeCallContext errors, Object value, Throwable exception) {
        context.getDataLoaderDispatcherStrategy().incrementalExecutionDone(errors);
        if (exception != null) {
            work.dispose();
            List<graphql.GraphQLError> reported = new ArrayList<>(errors.getErrors());
            if (reported.isEmpty()) reported.addAll(IncrementalErrors.from(exception, path));
            return new WorkResult(null, reported, work, true);
        }
        work.filter(value, path);
        return new WorkResult(value, errors.getErrors(), work, false);
    }

    private ExecutionStrategyParameters itemParameters(ExecutionStrategyParameters parameters, Object value, int index) {
        ResultPath path = parameters.getPath().segment(index);
        ExecutionStepInfo step = new ExecutionStepInfoFactory().newExecutionStepInfoForListElement(parameters.getExecutionStepInfo(), path);
        return parameters.transform(builder -> builder.executionStepInfo(step).path(path).source(value));
    }

    private void close(@Nullable AutoCloseable resource) {
        if (resource == null) return;
        try { resource.close(); } catch (Exception ignored) { }
    }
}
