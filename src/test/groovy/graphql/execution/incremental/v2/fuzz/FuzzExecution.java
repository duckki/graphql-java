package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.ExperimentalApi;
import graphql.Internal;
import graphql.incremental.IncrementalExecutionResult;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Internal
public final class FuzzExecution {
    private final ObjectMapper json = new ObjectMapper();

    public ObjectNode execute(FuzzInput input, FuzzSchema schema, String document, String demand, boolean incremental) throws Exception {
        Map<String, Object> variables = json.convertValue(input.variables, Map.class);
        Object root = json.convertValue(input.root, Object.class);
        String query = new FuzzQueryRenderer(schema.getSchema()).render(input.selections, document.equals("named"));
        ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).root(root).variables(variables)
                .graphQLContext(Map.of(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2, incremental, "FUZZ_SEED", input.seed)).build();
        ExecutionResult result = schema.graphQL.executeAsync(executionInput).get(5, TimeUnit.SECONDS);
        if (!result.isDataPresent()) throw new IllegalArgumentException("Generated input failed validation: " + result.getErrors());
        ArrayNode updates = json.createArrayNode();
        if (result instanceof IncrementalExecutionResult) updates = incrementalUpdates((IncrementalExecutionResult) result, demand);
        ObjectNode observation = json.createObjectNode();
        observation.set("initial", json.valueToTree(result.toSpecification()));
        observation.set("updates", updates);
        return observation;
    }

    private ArrayNode incrementalUpdates(IncrementalExecutionResult result, String demand) throws Exception {
        FuzzSubscriber subscriber = new FuzzSubscriber(demand);
        result.getIncrementalItemPublisher().subscribe(subscriber);
        try { subscriber.finished.get(5, TimeUnit.SECONDS); return subscriber.updates; }
        finally { subscriber.cancel(); }
    }
}
