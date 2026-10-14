package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;

@Internal
public final class FuzzInput {
    public final long seed;
    public final ArrayNode selections;
    public final ObjectNode variables;
    public final ObjectNode root;
    public final String query;
    public final String family;

    public FuzzInput(long seed, ArrayNode selections, ObjectNode variables, ObjectNode root) {
        this(seed, selections, variables, root, "legacy");
    }

    public FuzzInput(long seed, ArrayNode selections, ObjectNode variables, ObjectNode root, String family) {
        this.seed = seed;
        this.selections = selections;
        this.variables = variables;
        this.root = root;
        this.query = new FuzzQueryRenderer().render(selections);
        this.family = family;
    }

    public static FuzzInput fromJson(JsonNode input) {
        return new FuzzInput(input.path("seed").asLong(), (ArrayNode) input.get("selections"),
                (ObjectNode) input.get("variables"), (ObjectNode) input.get("root"), input.path("family").asText("corpus"));
    }
}
