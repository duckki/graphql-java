package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.Async;
import graphql.execution.ExecutionStrategyParameters;
import graphql.execution.DataLoaderDispatchStrategy;
import graphql.execution.MergedField;
import graphql.execution.incremental.AlternativeCallContext;
import graphql.execution.incremental.DeferredExecutionSupport;
import graphql.execution.incremental.IncrementalCall;
import graphql.incremental.IncrementalPayload;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static graphql.Assert.assertNotNull;

@Internal
public final class IncrementalExecutionSupport implements DeferredExecutionSupport {
    private final List<String> immediateNames;
    private final Set<String> immediateKeys;
    private final int deferredCount;
    private final DataLoaderDispatchStrategy dispatcher;

    public IncrementalExecutionSupport(ExecutionStrategyParameters parameters, DataLoaderDispatchStrategy dispatcher,
                                        Function<ExecutionStrategyParameters, Object> executeFields) {
        this.dispatcher = dispatcher;
        IncrementalExecutionParameters incremental = assertNotNull(parameters.getIncrementalExecutionParameters());
        ExecutionPlan plan = new ExecutionPlan(incremental.getCollectedFields(), incremental.getParentUsages());
        immediateNames = new ArrayList<>(plan.getImmediateFields().getFields().keySet());
        immediateKeys = new LinkedHashSet<>(immediateNames);
        deferredCount = parameters.getFields().size() - immediateNames.size();
        plan.getDeferredFields().forEach((owners, fields) -> addTask(parameters, incremental, owners, fields, executeFields));
    }

    private void addTask(ExecutionStrategyParameters parameters, IncrementalExecutionParameters incremental,
                         Set<DeferUsage> owners, CollectedFields fields, Function<ExecutionStrategyParameters, Object> executeFields) {
        List<DeliveryGroup> groups = new ArrayList<>();
        owners.forEach(usage -> groups.add(assertNotNull(incremental.getDeferGroups().get(usage))));
        IncrementalWork work = incremental.getWork().newChild();
        DeferredTask task = new DeferredTask(groups, parameters.getPath(), work,
                () -> executeTask(parameters, incremental, owners, fields, work, executeFields));
        incremental.getWork().addTask(task);
    }

    private CompletableFuture<WorkResult> executeTask(ExecutionStrategyParameters parameters, IncrementalExecutionParameters incremental,
                                                      Set<DeferUsage> owners, CollectedFields fields, IncrementalWork work,
                                                      Function<ExecutionStrategyParameters, Object> executeFields) {
        AlternativeCallContext errors = new AlternativeCallContext(parameters.getPath().getLevel() + 1, fields.getFields().size());
        IncrementalExecutionParameters taskContext = new IncrementalExecutionParameters(work, fields, owners, incremental.getDeferGroups());
        ExecutionStrategyParameters taskParameters = parameters.transform(builder -> builder
                .fields(fields.toMergedSelectionSet()).parent(null).alternativeCallContext(errors).incrementalExecutionParameters(taskContext));
        try {
            return Async.toCompletableFuture(executeFields.apply(taskParameters))
                    .handle((value, exception) -> finishTask(parameters, work, errors, value, exception));
        } catch (Exception exception) {
            return CompletableFuture.completedFuture(finishTask(parameters, work, errors, null, exception));
        }
    }

    private WorkResult finishTask(ExecutionStrategyParameters parameters, IncrementalWork work, AlternativeCallContext errors,
                                  Object value, Throwable exception) {
        dispatcher.incrementalExecutionDone(errors);
        boolean failed = exception != null || value == null;
        if (exception != null && errors.getErrors().isEmpty()) errors.addErrors(IncrementalErrors.from(exception, parameters.getPath()));
        if (failed) work.dispose();
        else work.filter(value, parameters.getPath());
        List<graphql.GraphQLError> diagnostics = List.copyOf(errors.getErrors());
        return new WorkResult(value, diagnostics, work, failed,
                exception == null ? null : IncrementalErrors.deliveryCause(exception, diagnostics, parameters.getPath()));
    }

    @Override
    public boolean isDeferredField(MergedField field) {
        return !immediateKeys.contains(field.getResultKey());
    }
    @Override
    public int deferredFieldsCount() {
        return deferredCount;
    }
    @Override
    public List<String> getNonDeferredFieldNames(List<String> fieldNames) {
        return immediateNames;
    }
    @Override
    public Set<IncrementalCall<? extends IncrementalPayload>> createCalls() {
        return Collections.emptySet();
    }
}
