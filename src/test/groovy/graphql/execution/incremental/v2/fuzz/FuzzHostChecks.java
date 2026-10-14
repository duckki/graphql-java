package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.ExperimentalApi;
import graphql.Internal;
import graphql.incremental.IncrementalExecutionResult;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Internal
public final class FuzzHostChecks {
    private final ObjectMapper json = new ObjectMapper();
    private final FuzzSchema schema;
    private final FuzzSemanticCoverage coverage;
    public ObjectNode observation;
    public FuzzInput rejectedInput;

    public FuzzHostChecks(FuzzSchema schema, FuzzSemanticCoverage coverage) { this.schema = schema; this.coverage = coverage; }

    public void checkRejectedItems(boolean strict, int prefix) throws Exception {
        observation = null;
        String name = strict ? "strictValues" : "values";
        ObjectNode directive = json.createObjectNode().put("name", "stream").put("if", true).put("initialCount", prefix);
        ObjectNode field = json.createObjectNode().put("kind", "field").put("name", name).put("responseName", name);
        field.set("directives", json.createArrayNode().add(directive));
        field.set("selections", json.createArrayNode());
        ObjectNode root = json.createObjectNode().put("__typename", "Query");
        root.set(name, json.createArrayNode().add(json.createObjectNode().put("$error", true)).add("after"));
        FuzzInput input = new FuzzInput(0, json.createArrayNode().add(field), json.createObjectNode(), root, "host-rejected-item");
        rejectedInput = input;
        FuzzHostState state = new FuzzHostState();
        ExecutionInput execution = ExecutionInput.newExecutionInput().query(input.query).root(json.convertValue(root, Object.class))
                .graphQLContext(Map.of(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2, true, "FUZZ_HOST", state)).build();
        ExecutionResult result = schema.graphQL.executeAsync(execution).get(5, TimeUnit.SECONDS);
        ArrayNode updates = drain(result);
        observation = json.createObjectNode().put("policy", -1);
        observation.set("initial", json.valueToTree(result.toSpecification()));
        observation.set("updates", updates);
        observation.set("sources", json.createArrayNode());
        ObjectNode finalResult = new FuzzObservation().reconstruct(observation.get("initial"), updates);
        JsonNode expected = strict ? prefix > 0 ? json.nullNode() : json.createArrayNode() : json.createArrayNode().addNull().add("after");
        require(finalResult.path("data").path(name).equals(expected) && finalResult.path("errors").asInt() == 1, "Rejected item must report one error and respect item nullability");
        verifySources(state, observation.get("initial"), updates);
        coverage.host("host.rejected-item." + (strict ? "fatal" : "nullable") + "." + prefix);
    }

    private ArrayNode drain(ExecutionResult result) throws Exception {
        if (!(result instanceof IncrementalExecutionResult)) return json.createArrayNode();
        FuzzSubscriber subscriber = new FuzzSubscriber();
        ((IncrementalExecutionResult) result).getIncrementalItemPublisher().subscribe(subscriber);
        subscriber.finished.get(5, TimeUnit.SECONDS);
        return subscriber.updates;
    }

    public void check(FuzzInput input) throws Exception {
        observation = null;
        int policy = (int) (input.seed % 6);
        FuzzHostState state = new FuzzHostState();
        ExecutionInput execution = ExecutionInput.newExecutionInput().query(new FuzzQueryRenderer(schema.getSchema()).render(input.selections, true))
                .variables(json.convertValue(input.variables, Map.class)).root(json.convertValue(input.root, Object.class))
                .graphQLContext(Map.of(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2, true, "FUZZ_SEED", input.seed, "FUZZ_HOST", state)).build();
        ExecutionResult result = schema.graphQL.executeAsync(execution).get(5, TimeUnit.SECONDS);
        ArrayNode updates = json.createArrayNode();
        if (result instanceof IncrementalExecutionResult) updates = cancel(execution, (IncrementalExecutionResult) result, policy);
        observation = json.createObjectNode().put("policy", policy);
        observation.set("initial", json.valueToTree(result.toSpecification()));
        observation.set("updates", updates);
        observation.set("sources", json.createArrayNode());
        new FuzzObservation().reconstructPrefix(observation.get("initial"), updates);
        verifySources(state, observation.get("initial"), updates);
    }

    private ArrayNode cancel(ExecutionInput input, IncrementalExecutionResult result, int policy) throws Exception {
        FuzzHostSubscriber subscriber = new FuzzHostSubscriber(input, policy);
        result.getIncrementalItemPublisher().subscribe(subscriber);
        if (policy == 5) rejectSecond(input, result);
        subscriber.finished.get(5, TimeUnit.SECONDS);
        int count = subscriber.updates.size();
        Thread.sleep(12);
        require(count == subscriber.updates.size(), "Payload arrived after cancellation");
        if (policy == 3) require(subscriber.error instanceof IllegalArgumentException, "Invalid demand must fail");
        if (policy == 4) require(subscriber.error != null, "Input cancellation must fail the publisher");
        coverage.host("host." + policy);
        return json.valueToTree(subscriber.updates);
    }

    private void rejectSecond(ExecutionInput input, IncrementalExecutionResult result) throws Exception {
        FuzzHostSubscriber rejected = new FuzzHostSubscriber(input, 1);
        result.getIncrementalItemPublisher().subscribe(rejected);
        rejected.finished.get(5, TimeUnit.SECONDS);
        require(rejected.error instanceof IllegalStateException, "Second subscriber must be rejected");
        coverage.host("host.second-subscriber");
    }

    private void verifySources(FuzzHostState state, JsonNode initial, JsonNode updates) {
        Map<String, JsonNode> paths = new LinkedHashMap<>();
        Map<String, Integer> published = new LinkedHashMap<>();
        initial.path("pending").forEach(notice -> paths.put(notice.path("id").asText(), notice.path("path")));
        for (JsonNode update : updates) countPublished(update, paths, published);
        for (FuzzTrackedIterable source : state.sources) verifySource(source, published);
    }

    private void countPublished(JsonNode update, Map<String, JsonNode> paths, Map<String, Integer> published) {
        update.path("pending").forEach(notice -> paths.put(notice.path("id").asText(), notice.path("path")));
        for (JsonNode patch : update.path("incremental")) {
            if (patch.has("items")) published.merge(paths.get(patch.path("id").asText()).toString(), patch.path("items").size(), Integer::sum);
        }
    }

    private void verifySource(FuzzTrackedIterable source, Map<String, Integer> published) {
        ObjectNode metrics = json.createObjectNode().put("acquired", source.acquired.get()).put("closes", source.closes.get())
                .put("pulls", source.pulls.get()).put("initialCount", source.initialCount).put("resource", source.closesIterator() ? "iterator" : "iterable");
        metrics.set("path", json.valueToTree(source.path));
        ((ArrayNode) observation.get("sources")).add(metrics);
        if (source.acquired.get() == 0) return;
        require(source.acquired.get() == 1, "Stream source acquired more than once");
        require(source.closes.get() == 1, "Acquired source must close exactly once");
        int prefix = Math.min(source.size(), source.initialCount);
        int bound = prefix + published.getOrDefault(json.valueToTree(source.path).toString(), 0) + 1;
        require(source.pulls.get() <= bound, "Stream exceeded its one-item prefetch window");
        coverage.host("host.closed-source");
        coverage.host(source.closesIterator() ? "host.closed-iterator" : "host.closed-iterable");
    }

    private void require(boolean valid, String message) { if (!valid) throw new DifferentialMismatch(message); }
}
