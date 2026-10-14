package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;

@Internal
public final class FuzzInputGenerator {
    private final JsonNodeFactory json = JsonNodeFactory.instance;
    private final int[] bytes = new int[256];
    private int offset = 8;
    private int labels;
    private int budget = 36;
    private boolean errors;
    private int fallbackReads;

    public FuzzInput generate(long seed) {
        int state = (int) seed;
        if (state == 0) state = 0x9e3779b9;
        for (int index = 0; index < bytes.length; index++) {
            state ^= state << 13;
            state ^= state >>> 17;
            state ^= state << 5;
            bytes[index] = state & 255;
        }
        offset = 8;
        labels = 0;
        budget = 36;
        fallbackReads = 0;
        ArrayNode selections = bytes[6] % 2 != 0 ? structural("Query", 3) : template(bytes[0] % 8);
        ObjectNode variables = json.objectNode().put("x", bytes[2] % 2 == 0).put("y", bytes[3] % 2 == 0).put("n", bytes[1] % 5);
        errors = bytes[4] % 4 == 3;
        return new FuzzInput(seed, selections, variables, root());
    }

    public int getFallbackReads() { return fallbackReads; }
    private int next() {
        if (offset < bytes.length) return bytes[offset++];
        fallbackReads++;
        return 0;
    }
    private ArrayNode nodes(JsonNode... values) { ArrayNode result = json.arrayNode(); for (JsonNode value : values) result.add(value); return result; }
    private JsonNode variable(String name) { return json.objectNode().put("variable", name); }
    private JsonNode condition() {
        int choice = next() % 5;
        if (choice < 3) return json.booleanNode(choice != 2);
        return variable(choice == 3 ? "x" : "y");
    }

    private ObjectNode directive(String name, JsonNode count) {
        ObjectNode result = json.objectNode().put("name", name);
        result.set("if", condition());
        if (name.equals("defer") || name.equals("stream")) result.put("label", "l" + labels++);
        if (name.equals("stream")) result.set("initialCount", count == null ? streamCount() : count);
        return result;
    }

    private JsonNode streamCount() { int choice = next() % 6; return choice == 5 ? variable("n") : json.numberNode(choice); }
    private ArrayNode defer() { return nodes(directive("defer", null)); }
    private ArrayNode stream() { return nodes(directive("stream", null)); }

    private ObjectNode field(String name, ArrayNode children, ArrayNode directives, String alias) {
        ObjectNode result = json.objectNode().put("kind", "field").put("name", name).put("responseName", alias);
        result.set("selections", children);
        result.set("directives", directives);
        return result;
    }
    private ObjectNode field(String name) { return field(name, nodes(), nodes(), name); }
    private ObjectNode field(String name, ArrayNode children) { return field(name, children, nodes(), name); }
    private ObjectNode streamed(String name) { return field(name, nodes(), stream(), name); }
    private ObjectNode fragment(ArrayNode children) { return fragment(children, defer(), null); }
    private ObjectNode fragment(ArrayNode children, ArrayNode directives, String type) {
        ObjectNode result = json.objectNode().put("kind", "fragment");
        result.set("selections", children); result.set("directives", directives);
        if (type != null) result.put("typeCondition", type);
        return result;
    }
    private ArrayNode item() { return nodes(field("id"), fragment(nodes(field("name"))), streamed("values")); }

    private ArrayNode template(int choice) {
        switch (choice) {
            case 0: return nodes(field("a"), field("user", nodes(field("id"), field("name"))));
            case 1: return nodes(field("values", nodes(), nodes(directive("stream", json.numberNode(bytes[1] % 5))), "values"), streamed("strictValues"));
            case 2: return nodes(field("a"), fragment(nodes(field("b"), field("required"))));
            case 3: return nodes(field("a"), fragment(nodes(field("a"), field("b"))), fragment(nodes(field("b"), field("user", item()))));
            case 4: return deepDefer();
            case 5: return nodes(field("users", item(), stream(), "users"), fragment(nodes(streamed("values"))));
            case 6: return sharedUsers();
            default: return nodes(field("strictUsers", nodes(field("id"), fragment(nodes(field("required")))), stream(), "strictUsers"), fragment(nodes(field("required"))));
        }
    }

    private ArrayNode deepDefer() {
        ObjectNode inner = fragment(nodes(field("name"), field("required")));
        ObjectNode outer = fragment(nodes(field("name"), inner));
        return nodes(fragment(nodes(field("user", nodes(field("id"), outer)))));
    }

    private ArrayNode sharedUsers() {
        ObjectNode first = fragment(nodes(field("users", nodes(field("id"), fragment(nodes(field("name")))))));
        ObjectNode friends = field("friends", item(), stream(), "friends");
        ObjectNode second = fragment(nodes(field("users", nodes(field("name"), friends))));
        return nodes(first, second);
    }

    private ArrayNode structural(String type, int depth) {
        ArrayNode selections = nodes();
        int count = 1 + next() % 4;
        for (int index = 0; index < count && budget > 0; index++) selections.add(structuralSelection(type, depth));
        if (selections.isEmpty()) selections.add(field(type.equals("Query") ? "a" : "id"));
        return selections;
    }

    private JsonNode structuralSelection(String type, int depth) {
        budget--;
        if (depth > 0 && next() % 4 == 3) {
            ArrayNode children = structural(type, depth - 1);
            ArrayNode directives = defer();
            return fragment(children, directives, next() % 2 != 0 ? type : null);
        }
        String[] fields = type.equals("Query") ? new String[]{"a", "b", "required", "values", "strictValues", "user", "users", "strictUsers"}
                : new String[]{"id", "name", "required", "values", "strictValues", "friend", "friends"};
        String name = fields[next() % (depth == 0 ? 3 : fields.length)];
        boolean composite = name.equals("user") || name.endsWith("Users") || name.equals("users") || name.equals("friend") || name.equals("friends");
        boolean list = name.equals("users") || name.endsWith("Users") || name.equals("friends") || name.equals("values") || name.equals("strictValues");
        ArrayNode directives = nodes();
        if (list && next() % 3 != 0) directives.add(directive("stream", null));
        if (next() % 5 == 4) directives.add(directive(next() % 2 == 0 ? "skip" : "include", null));
        String alias = list ? name + "_" + budget : next() % 4 == 3 ? "alias_" + name : name;
        return field(name, composite ? structural("Item", depth - 1) : nodes(), directives, alias);
    }

    private JsonNode scalar(String path) {
        if (errors && next() % 7 == 6) return json.objectNode().put("$error", true);
        if (next() % 9 == 8 && (errors || !path.endsWith("id") && !path.endsWith("required"))) return json.nullNode();
        return json.textNode(path);
    }

    private ArrayNode strings(String path) {
        ArrayNode result = nodes();
        int count = next() % 4;
        for (int index = 0; index < count; index++) result.add(next() % 6 == 5 && (errors || !path.endsWith("strictValues")) ? json.nullNode() : json.textNode(path + "/" + index));
        return result;
    }

    private ObjectNode object(String path, int depth) {
        ObjectNode result = json.objectNode().put("__typename", "Item");
        result.set("id", scalar(path + "/id")); result.set("name", scalar(path + "/name")); result.set("required", scalar(path + "/required"));
        result.set("values", strings(path + "/values")); result.set("strictValues", strings(path + "/strictValues"));
        result.set("friend", depth > 0 && next() % 2 != 0 ? object(path + "/friend", depth - 1) : json.nullNode());
        ArrayNode friends = nodes(); int count = depth > 0 ? next() % 3 : 0;
        for (int index = 0; index < count; index++) friends.add(object(path + "/friends/" + index, depth - 1));
        result.set("friends", friends); return result;
    }

    private ArrayNode objects(String path) {
        ArrayNode result = nodes(); int count = next() % 4;
        for (int index = 0; index < count; index++) result.add(next() % 6 == 5 && (errors || !path.equals("strictUsers")) ? json.nullNode() : object(path + "/" + index, 2));
        return result;
    }

    private ObjectNode root() {
        ObjectNode result = json.objectNode().put("__typename", "Query");
        result.set("a", scalar("a")); result.set("b", scalar("b")); result.set("required", scalar("required"));
        result.set("values", strings("values")); result.set("strictValues", strings("strictValues"));
        result.set("user", object("user", 2)); result.set("users", objects("users")); result.set("strictUsers", objects("strictUsers"));
        return result;
    }
}
