package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import graphql.Internal;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLTypeUtil;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

@Internal
public final class FuzzQueryRenderer {
    private final Set<String> variables = new LinkedHashSet<>();
    private final Map<String, String> fragmentNames = new LinkedHashMap<>();
    private final Map<String, String> definitions = new LinkedHashMap<>();
    private boolean named;
    private final GraphQLSchema schema;

    public FuzzQueryRenderer() { this.schema = null; }
    public FuzzQueryRenderer(GraphQLSchema schema) { this.schema = schema; }

    public String render(JsonNode selections) {
        return render(selections, false);
    }

    public String render(JsonNode selections, boolean named) {
        this.named = named;
        variables.clear();
        fragmentNames.clear();
        definitions.clear();
        String body = selections(selections, "Query");
        StringJoiner definitions = new StringJoiner(", ");
        variables.forEach(name -> definitions.add("$" + name + ": " + (name.equals("n") ? "Int! = 0" : "Boolean! = true")));
        return "query" + (variables.isEmpty() ? "" : "(" + definitions + ")") + " { " + body + " } " + String.join(" ", this.definitions.values());
    }

    private String selections(JsonNode nodes, String type) {
        StringJoiner result = new StringJoiner(" ");
        nodes.forEach(node -> result.add(selection(node, type)));
        return result.toString();
    }

    private String selection(JsonNode node, String type) {
        String head = node.path("kind").asText().equals("fragment") ? fragment(node) : field(node);
        StringJoiner directives = new StringJoiner(" ");
        node.path("directives").forEach(directive -> directives.add(directive(directive)));
        String childType = childType(node, type);
        String body = selections(node.path("selections"), childType);
        if (named && node.path("kind").asText().equals("fragment")) return "..." + namedFragment(childType, body) + " " + directives;
        String children = body.isEmpty() ? "" : " { " + (named ? "..." + namedFragment(childType, body) : body) + " }";
        return head + " " + directives + children;
    }

    private String childType(JsonNode node, String type) {
        if (node.path("kind").asText().equals("fragment")) return node.path("typeCondition").asText(type);
        if (schema == null || node.path("selections").isEmpty()) return "Item";
        GraphQLFieldsContainer parent = (GraphQLFieldsContainer) schema.getType(type);
        return ((GraphQLNamedType) GraphQLTypeUtil.unwrapAll(parent.getFieldDefinition(node.path("name").asText()).getType())).getName();
    }

    private String namedFragment(String type, String body) {
        String key = type + ":" + body;
        if (fragmentNames.containsKey(key)) return fragmentNames.get(key);
        String name = "F" + fragmentNames.size();
        fragmentNames.put(key, name);
        definitions.put(name, "fragment " + name + " on " + type + " { " + body + " }");
        return name;
    }

    private String fragment(JsonNode node) {
        return "..." + (node.has("typeCondition") ? " on " + node.get("typeCondition").asText() : "");
    }

    private String field(JsonNode node) {
        String name = node.path("name").asText();
        String responseName = node.path("responseName").asText();
        return name.equals(responseName) ? name : responseName + ": " + name;
    }

    private String directive(JsonNode directive) {
        String name = directive.path("name").asText();
        String result = "@" + name + "(if: " + value(directive.get("if"));
        if (directive.has("label")) result += ", label: " + value(directive.get("label"));
        return result + ")";
    }

    private String value(JsonNode value) {
        if (!value.isObject()) return value.toString();
        String name = value.path("variable").asText();
        variables.add(name);
        return "$" + name;
    }
}
