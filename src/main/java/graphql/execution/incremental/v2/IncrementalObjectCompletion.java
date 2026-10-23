package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ExecutionStrategyParameters;
import graphql.execution.FieldCollectorParameters;
import graphql.execution.MergedField;
import graphql.execution.incremental.AlternativeCallContext;
import graphql.schema.GraphQLObjectType;

import java.util.function.Function;

import static graphql.Assert.assertNotNull;

@Internal
public final class IncrementalObjectCompletion {
    public Object complete(ExecutionContext context, ExecutionStrategyParameters parameters, GraphQLObjectType objectType,
                            Object value, FieldCollectorParameters collector, Function<ExecutionStrategyParameters, Object> executeObject) {
        IncrementalExecutionParameters incremental = assertNotNull(parameters.getIncrementalExecutionParameters());
        incremental.getWork().checkRunning();
        MergedField field = assertNotNull(parameters.getField());
        CollectedFields fields = new IncrementalFieldCollector().collectSubfields(collector,
                assertNotNull(incremental.getCollectedFields().getFields().get(field.getResultKey())));
        IncrementalExecutionParameters child = incremental.withFields(fields, parameters.getPath());
        AlternativeCallContext alternative = parameters.getAlternativeCallContext();
        if (alternative instanceof StreamCallContext) {
            int count = new ExecutionPlan(fields, incremental.getParentUsages()).getImmediateFields().getFields().size();
            ((StreamCallContext) alternative).configure(parameters.getPath(), count);
        }
        ExecutionStepInfo step = parameters.getExecutionStepInfo().changeTypeWithPreservedNonNull(objectType);
        ExecutionStrategyParameters childParameters = parameters.transform(builder -> builder
                .executionStepInfo(step).fields(fields.toMergedSelectionSet()).source(value).incrementalExecutionParameters(child));
        Object result = executeObject.apply(childParameters);
        child.declareGroups();
        return result;
    }
}
