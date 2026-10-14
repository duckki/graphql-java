package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;

@Internal
public final class FuzzComparison {
    private final ObjectMapper json = new ObjectMapper();
    private final LeanOracle oracle;
    private final FuzzSchema schema;
    private final String document;
    private final String demand;
    public ObjectNode actual;
    public ObjectNode expected;
    public JsonNode expectedSummary;
    public JsonNode actualSummary;

    public FuzzComparison(LeanOracle oracle, FuzzSchema schema) { this(oracle, schema, "inline", "one"); }
    public FuzzComparison(LeanOracle oracle, FuzzSchema schema, String document, String demand) {
        this.oracle = oracle;
        this.schema = schema;
        this.document = document;
        this.demand = demand;
    }

    public void check(FuzzInput input) throws Exception {
        actual = null;
        expected = null;
        actualSummary = null;
        expectedSummary = null;
        JsonNode modeled = oracle.query(input, schema.modelSchema());
        if (!modeled.has("schedule") || !modeled.path("executionPolicy").asText().equals("eager")) {
            throw new java.io.IOException("Oracle does not implement the required eager auto-schedule protocol");
        }
        for (JsonNode admitted : modeled.path("admission")) {
            if (admitted.isBoolean() && !admitted.asBoolean()) throw new java.io.IOException("Oracle selected an inadmissible schedule");
        }
        ObjectNode initial = modeled.path("response").deepCopy();
        initial.set("pending", modeled.path("initialPending"));
        if (!initial.path("pending").isEmpty()) initial.put("hasNext", true);
        ArrayNode updates = json.createArrayNode();
        modeled.path("steps").forEach(step -> { if (!step.path("wire").isNull()) updates.add(step.get("wire")); });
        expected = observation(initial, updates);
        expectedSummary = new FuzzObservation().reconstruct(initial, updates);
        actual = new FuzzExecution().execute(input, schema, document, demand, true);
        actualSummary = new FuzzObservation().reconstruct(actual.get("initial"), (ArrayNode) actual.get("updates"));
        if (!initial.path("data").equals(actual.path("initial").path("data"))) throw new DifferentialMismatch("Initial data differs from Lean");
        if (initial.path("errors").asInt() != actual.path("initial").path("errors").size()) throw new DifferentialMismatch("Initial error count differs from Lean");
        assertEqual(actualSummary, expectedSummary);
    }

    private ObjectNode observation(JsonNode initial, JsonNode updates) {
        ObjectNode result = json.createObjectNode(); result.set("initial", initial); result.set("updates", updates); return result;
    }

    public void assertEqual(JsonNode actual, JsonNode expected) {
        if (!actual.equals(expected)) throw new DifferentialMismatch("Reconstructed data or counted errors differ from Lean: actual=" + actual + ", expected=" + expected);
    }
}
