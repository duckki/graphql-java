package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Internal
public final class FuzzFaultChecks {
    private final ObjectMapper json = new ObjectMapper();
    private final FuzzSchema schema;
    private final String document;
    private final String demand;
    public ObjectNode observation;

    public FuzzFaultChecks(FuzzSchema schema, String document, String demand) {
        this.schema = schema;
        this.document = document;
        this.demand = demand;
    }

    public void check(FuzzInput input) throws Exception {
        observation = json.createObjectNode();
        FuzzExecution execution = new FuzzExecution();
        ObjectNode incremental = execution.execute(input, schema, document, demand, true);
        observation.set("incremental", incremental);
        summarize(incremental);
        checkOrigins(input, incremental);
        FuzzInput ordinary = new FuzzInput(input.seed, strip(input.selections), input.variables, input.root, input.family);
        ObjectNode modern = execution.execute(ordinary, schema, document, demand, true);
        ObjectNode existing = execution.execute(ordinary, schema, document, demand, false);
        observation.set("ordinaryV2", modern);
        observation.set("ordinaryExisting", existing);
        if (!summarize(modern).equals(summarize(existing)) || !errorPaths(modern).equals(errorPaths(existing))) {
            throw new DifferentialMismatch("Ordinary new/existing execution differs for multi-failure input");
        }
    }

    public void checkOrigins(FuzzInput input, JsonNode result) {
        FuzzErrorOrigins origins = new FuzzErrorOrigins(input, schema.getSchema());
        Set<String> emitted = new LinkedHashSet<>();
        errors(result.path("initial").path("errors"), "initial", origins, emitted);
        for (JsonNode update : result.path("updates")) updateErrors(update, origins, emitted);
    }

    private void updateErrors(JsonNode update, FuzzErrorOrigins origins, Set<String> emitted) {
        for (JsonNode payload : update.path("incremental")) errors(payload.path("errors"), payload.path("id").asText(), origins, emitted);
        for (JsonNode payload : update.path("completed")) errors(payload.path("errors"), payload.path("id").asText(), origins, emitted);
    }

    private void errors(JsonNode errors, String boundary, FuzzErrorOrigins origins, Set<String> emitted) {
        for (JsonNode error : errors) checkError(error, boundary, origins, emitted);
    }

    private void checkError(JsonNode error, String boundary, FuzzErrorOrigins origins, Set<String> emitted) {
        origins.check(error);
        if (!emitted.add(boundary + ":" + error.path("path"))) throw new DifferentialMismatch("Duplicate primitive error in delivery boundary: " + error);
    }

    private JsonNode summarize(JsonNode result) {
        return new FuzzObservation().reconstruct(result.path("initial"), (ArrayNode) result.path("updates"));
    }

    private List<String> errorPaths(JsonNode result) {
        List<String> paths = new ArrayList<>();
        result.path("initial").path("errors").forEach(error -> paths.add(error.path("path").toString()));
        Collections.sort(paths);
        return paths;
    }

    private ArrayNode strip(JsonNode selections) {
        ArrayNode result = json.createArrayNode();
        selections.forEach(node -> result.add(stripNode(node)));
        return result;
    }

    private ObjectNode stripNode(JsonNode original) {
        ObjectNode node = original.deepCopy();
        ArrayNode directives = json.createArrayNode();
        original.path("directives").forEach(directive -> { if (!List.of("defer", "stream").contains(directive.path("name").asText())) directives.add(directive); });
        node.set("directives", directives);
        node.set("selections", strip(original.path("selections")));
        return node;
    }
}
