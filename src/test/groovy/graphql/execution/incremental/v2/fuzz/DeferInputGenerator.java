package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;
import graphql.schema.GraphQLSchema;

import java.util.SplittableRandom;

@Internal
public final class DeferInputGenerator {
    private final JsonNodeFactory json = JsonNodeFactory.instance;
    private final GraphQLSchema schema;
    private SplittableRandom random;
    private int labels;
    private int aliases;
    private int budget;

    public DeferInputGenerator(GraphQLSchema schema) {
        this.schema = schema;
    }

    public FuzzInput generate(long seed) {
        random = new SplittableRandom(seed);
        labels = 0;
        aliases = 0;
        budget = 36;
        int family = (int) Math.floorMod(seed, 6);
        ArrayNode selections = scenario(family);
        ObjectNode variables = json.objectNode().put("x", random.nextBoolean()).put("y", random.nextBoolean());
        ObjectNode root = new FuzzDataGenerator(schema, seed, 3, family < 2, false).generate(schema.getQueryType(), selections, "");
        if (family == 5) root.set("user", json.objectNode().put("__typename", "Item").put("id", "1").putNull("required"));
        return new FuzzInput(seed, selections, variables, root, "defer-" + family);
    }

    private ArrayNode scenario(int family) {
        if (family < 2) return structural("Query", 3);
        if (family == 2) return shared();
        if (family == 3) return deep();
        if (family == 4) return conditions();
        return nodes(field("user", nodes(field("id", nodes()), deferred(nodes(field("required", nodes()))))));
    }

    private ArrayNode shared() {
        ArrayNode result = nodes();
        int count = 2 + random.nextInt(4);
        for (int index = 0; index < count; index++) result.add(deferred(nodes(field("a", nodes()), field("user", nodes(field("id", nodes()), field("name", nodes()))))));
        return result;
    }

    private ArrayNode deep() {
        ArrayNode children = nodes(field("name", nodes()));
        int depth = 2 + random.nextInt(4);
        for (int index = 0; index < depth; index++) children = nodes(deferred(children));
        return nodes(field("user", nodes(field("id", nodes()), deferred(children))));
    }

    private ArrayNode conditions() {
        ObjectNode fragment = deferred(nodes(field("b", nodes()), field("users", nodes(field("id", nodes()), deferred(nodes(field("name", nodes())))))));
        ((ArrayNode) fragment.get("directives")).add(directive("skip", json.objectNode().put("variable", "x")));
        ((ArrayNode) fragment.get("directives")).add(directive("include", json.objectNode().put("variable", "y")));
        return nodes(field("a", nodes()), fragment);
    }

    private ArrayNode structural(String type, int depth) {
        ArrayNode result = nodes();
        int count = 1 + random.nextInt(3);
        for (int index = 0; index < count && budget > 0; index++) result.add(selection(type, depth));
        if (result.isEmpty()) result.add(field(type.equals("Query") ? "a" : "id", nodes()));
        return result;
    }

    private ObjectNode selection(String type, int depth) {
        budget--;
        if (depth > 0 && random.nextInt(4) == 0) return deferred(structural(type, depth - 1));
        String[] names = type.equals("Query") ? new String[]{"a", "b", "required", "user", "users", "values"}
                : new String[]{"id", "name", "required", "friend", "friends", "values"};
        String name = names[random.nextInt(depth == 0 ? 3 : names.length)];
        boolean object = name.equals("user") || name.equals("users") || name.equals("friend") || name.equals("friends");
        ObjectNode field = field(name, object ? structural("Item", depth - 1) : nodes());
        if (random.nextInt(4) == 0) field.put("responseName", "alias" + aliases++);
        if (random.nextInt(5) == 0) ((ArrayNode) field.get("directives")).add(directive("include", json.objectNode().put("variable", "x")));
        return field;
    }

    private ObjectNode field(String name, ArrayNode children) {
        ObjectNode result = json.objectNode().put("kind", "field").put("name", name).put("responseName", name);
        result.set("selections", children);
        result.set("directives", nodes());
        return result;
    }

    private ObjectNode deferred(ArrayNode children) {
        ObjectNode result = json.objectNode().put("kind", "fragment");
        ObjectNode directive = directive("defer", random.nextInt(5) == 0 ? json.objectNode().put("variable", "x") : json.booleanNode(true));
        directive.put("label", "label" + labels++);
        result.set("directives", nodes(directive));
        result.set("selections", children);
        return result;
    }

    private ObjectNode directive(String name, JsonNode condition) {
        ObjectNode result = json.objectNode().put("name", name);
        result.set("if", condition);
        return result;
    }

    private ArrayNode nodes(JsonNode... nodes) {
        ArrayNode result = json.arrayNode();
        for (JsonNode node : nodes) result.add(node);
        return result;
    }
}
