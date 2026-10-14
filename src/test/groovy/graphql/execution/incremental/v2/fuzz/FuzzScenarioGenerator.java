package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;
import graphql.schema.GraphQLSchema;

import java.util.SplittableRandom;

@Internal
public final class FuzzScenarioGenerator {
    public static final String[] FAMILIES = {"structural", "shared-owners", "deep-defer", "prefix-boundary", "nested-list",
            "stream-children", "fatal-tail", "deferred-cut", "conditions", "shared-composite", "abstract-defer", "abstract-stream"};
    private final JsonNodeFactory json = JsonNodeFactory.instance;
    private final FuzzProfile profile;
    private final GraphQLSchema schema;
    private SplittableRandom random;
    private int labels;
    private int aliases;
    private int budget;

    public FuzzScenarioGenerator(FuzzProfile profile, GraphQLSchema schema) {
        this.profile = profile;
        this.schema = schema;
    }

    public FuzzInput generate(long seed) {
        return generate(seed, false);
    }

    public FuzzInput generateFaults(long seed) {
        return generate(seed, true);
    }

    private FuzzInput generate(long seed, boolean fatalErrors) {
        random = new SplittableRandom(seed);
        labels = 0;
        aliases = 0;
        budget = profile.selections;
        int slot = (int) (seed % 55);
        int family = slot < 22 ? 0 : 1 + (slot - 22) / 3;
        ArrayNode selections = scenario(family);
        ObjectNode variables = json.objectNode().put("x", random.nextBoolean()).put("y", random.nextBoolean()).put("n", random.nextInt(profile.listLength + 3));
        ObjectNode root = new FuzzDataGenerator(schema, seed, profile.listLength, family == 0 && random.nextBoolean(), fatalErrors).generate(schema.getQueryType(), selections, "");
        targetData(family, root);
        return new FuzzInput(seed, selections, variables, root, FAMILIES[family]);
    }

    private ArrayNode scenario(int family) {
        switch (family) {
            case 0: return structural("Query", profile.depth);
            case 1: return sharedOwners();
            case 2: return nodes(field("user", deepDefer(profile.depth + random.nextInt(3))));
            case 3: return nodes(streamed("values", nodes(), count()));
            case 4: return nodes(streamed(random.nextBoolean() ? "matrix" : "strictMatrix", nodes(), count()));
            case 5: return nodes(streamed("users", nodes(field("id"), deferred(nodes(field("name"), streamed("values", nodes(), count())))), count()));
            case 6: return nodes(streamed("strictUsers", nodes(field("id"), deferred(nodes(field("required"), streamed("values", nodes(), json.numberNode(0))))), json.numberNode(1)));
            case 7: return nodes(deferred(nodes(field("user", nodes(field("required"), deferred(nodes(streamed("values", nodes(), json.numberNode(0)))))))));
            case 8: return conditions();
            case 9: return sharedComposite();
            case 10: return nodes(field("node", abstractFields()));
            default: return nodes(streamed(random.nextBoolean() ? "nodes" : "search", abstractFields(), count()));
        }
    }

    private ArrayNode abstractFields() {
        ObjectNode common = deferred(nodes(field("name")));
        common.put("typeCondition", "Named");
        ObjectNode item = deferred(nodes(field("itemOnly"), field("friend", nodes(field("id")))));
        item.put("typeCondition", "Item");
        ObjectNode other = deferred(nodes(field("otherOnly")));
        other.put("typeCondition", "Other");
        ObjectNode id = json.objectNode().put("kind", "fragment").put("typeCondition", "Node");
        id.set("directives", nodes());
        id.set("selections", nodes(field("id")));
        return nodes(id, common, item, other);
    }

    private ArrayNode sharedOwners() {
        ArrayNode result = nodes();
        int owners = 2 + random.nextInt(5);
        for (int index = 0; index < owners; index++) result.add(sharedOwner(index));
        return result;
    }

    private ObjectNode sharedOwner(int index) {
        ObjectNode stream = field("values", nodes(), nodes(directive("stream", json.booleanNode(true), json.numberNode(0))), "values_" + index);
        return deferred(nodes(field(index % 2 == 0 ? "a" : "b"), field("user", nodes(field("name"), field("id"), stream)), field("required")));
    }

    private ArrayNode deepDefer(int depth) {
        ArrayNode result = nodes(field("name"));
        for (int index = 0; index < depth; index++) result = nodes(deferred(result));
        return nodes(field("id"), deferred(result));
    }

    private ArrayNode conditions() {
        ObjectNode fragment = deferred(nodes(field("a"), field("user", nodes(field("name")))));
        ((ArrayNode) fragment.get("directives")).add(directive("skip", variable("x"), null));
        ((ArrayNode) fragment.get("directives")).add(directive("include", variable("y"), null));
        return nodes(field("b"), fragment, field("values", nodes(), nodes(directive("stream", variable("x"), variable("n"))), "values"));
    }

    private ArrayNode sharedComposite() {
        ObjectNode first = deferred(nodes(field("users", nodes(field("id"), deferred(nodes(field("name")))))));
        ObjectNode second = deferred(nodes(field("users", nodes(field("name"), field("friend", nodes(field("id"), deferred(nodes(field("name")))))))));
        return nodes(first, second);
    }

    private ArrayNode structural(String type, int depth) {
        ArrayNode result = nodes();
        int count = 1 + random.nextInt(5);
        for (int index = 0; index < count && budget > 0; index++) result.add(selection(type, depth));
        if (result.isEmpty()) result.add(type.equals("Search") ? idFragment() : field(type.equals("Query") ? "a" : "id"));
        return result;
    }

    private ObjectNode idFragment() {
        ObjectNode result = json.objectNode().put("kind", "fragment").put("typeCondition", "Node");
        result.set("directives", nodes());
        result.set("selections", nodes(field("id")));
        return result;
    }

    private JsonNode selection(String type, int depth) {
        budget--;
        if (type.equals("Search")) return unionSelection(depth);
        if (depth > 0 && random.nextInt(4) == 0) {
            ObjectNode fragment = deferred(structural(type, depth - 1));
            if (random.nextBoolean()) fragment.put("typeCondition", type);
            ((ObjectNode) fragment.path("directives").get(0)).set("if", condition());
            return fragment;
        }
        String[] names = fieldNames(type);
        String name = names[random.nextInt(depth == 0 ? Math.min(3, names.length) : names.length)];
        boolean composite = name.contains("User") || name.equals("user") || name.equals("users") || name.equals("friend") || name.equals("friends") || name.equals("node") || name.equals("nodes") || name.equals("search");
        boolean list = name.endsWith("s") && !name.equals("mandatoryUser") || name.contains("Matrix") || name.equals("matrix") || name.equals("search");
        String childType = name.equals("node") || name.equals("nodes") ? "Named" : name.equals("search") ? "Search" : "Item";
        ArrayNode children = composite ? structural(childType, depth - 1) : nodes();
        ArrayNode directives = nodes();
        if (list && random.nextBoolean()) directives.add(directive("stream", condition(), count()));
        if (random.nextInt(4) == 0) directives.add(directive(random.nextBoolean() ? "skip" : "include", condition(), null));
        return field(name, children, directives, list ? name + "_" + aliases++ : name);
    }

    private String[] fieldNames(String type) {
        if (type.equals("Query")) return new String[]{"a", "b", "required", "values", "strictValues", "matrix", "strictMatrix", "user", "users", "strictUsers", "mandatoryUser", "mandatoryUsers", "node", "nodes", "search"};
        if (type.equals("Named")) return new String[]{"id", "name"};
        if (type.equals("Other")) return new String[]{"id", "name", "otherOnly"};
        return new String[]{"id", "name", "required", "values", "strictValues", "matrix", "strictMatrix", "friend", "friends", "itemOnly"};
    }

    private ObjectNode unionSelection(int depth) {
        String type = random.nextBoolean() ? "Item" : "Other";
        ObjectNode result = json.objectNode().put("kind", "fragment").put("typeCondition", type);
        result.set("directives", random.nextBoolean() ? nodes(directive("defer", condition(), null)) : nodes());
        result.set("selections", structural(type, Math.max(0, depth - 1)));
        return result;
    }

    private void targetData(int family, ObjectNode root) {
        if (family == 1 && random.nextBoolean()) {
            root.putNull("required");
            root.set("user", item("shared", false));
            return;
        }
        if (family == 3) {
            ArrayNode values = strings(profile.listLength);
            root.set("values", values);
            return;
        }
        if (family == 6) {
            ArrayNode users = nodes(item("first", false), item("fatal", true), item("unvisited", false));
            root.set("strictUsers", users);
            return;
        }
        if (family == 7) root.set("user", item("parent-cut", true));
    }

    private ObjectNode item(String id, boolean failed) {
        ObjectNode result = json.objectNode().put("__typename", "Item").put("id", id).put("name", id);
        result.set("required", failed ? json.nullNode() : json.textNode(id));
        result.set("values", strings(3));
        return result;
    }

    private ArrayNode strings(int count) {
        ArrayNode result = nodes();
        for (int index = 0; index < count; index++) result.add("value-" + index);
        return result;
    }

    private JsonNode count() {
        int choice = random.nextInt(5);
        if (choice == 0) return json.numberNode(0);
        if (choice == 1) return json.numberNode(1);
        if (choice == 2) return json.numberNode(profile.listLength);
        if (choice == 3) return json.numberNode(profile.listLength + 2);
        return variable("n");
    }

    private JsonNode condition() {
        int choice = random.nextInt(5);
        if (choice < 3) return json.booleanNode(choice < 2);
        return variable(choice == 3 ? "x" : "y");
    }

    private ObjectNode directive(String name, JsonNode condition, JsonNode count) {
        ObjectNode directive = json.objectNode().put("name", name);
        directive.set("if", condition);
        if (name.equals("defer") || name.equals("stream")) label(directive);
        if (name.equals("stream")) directive.set("initialCount", count);
        return directive;
    }

    private void label(ObjectNode directive) {
        int choice = random.nextInt(3);
        if (choice == 0) directive.put("label", "l" + labels++);
        if (choice == 1) directive.putNull("label");
    }

    private ObjectNode deferred(ArrayNode selections) {
        ObjectNode node = json.objectNode().put("kind", "fragment");
        node.set("selections", selections);
        node.set("directives", nodes(directive("defer", json.booleanNode(true), null)));
        return node;
    }

    private ObjectNode streamed(String name, ArrayNode selections, JsonNode count) {
        return field(name, selections, nodes(directive("stream", json.booleanNode(true), count)), name);
    }

    private ObjectNode field(String name) { return field(name, nodes()); }
    private ObjectNode field(String name, ArrayNode selections) { return field(name, selections, nodes(), name); }
    private ObjectNode field(String name, ArrayNode selections, ArrayNode directives, String alias) {
        ObjectNode node = json.objectNode().put("kind", "field").put("name", name).put("responseName", alias);
        node.set("selections", selections);
        node.set("directives", directives);
        return node;
    }

    private ArrayNode nodes(JsonNode... nodes) {
        ArrayNode result = json.arrayNode();
        for (JsonNode node : nodes) result.add(node);
        return result;
    }

    private JsonNode variable(String name) { return json.objectNode().put("variable", name); }
}
