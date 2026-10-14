package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import graphql.Internal;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLUnionType;
import graphql.schema.GraphQLNamedType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Internal
public final class FuzzErrorOrigins {
    private final Map<String, String> origins = new LinkedHashMap<>();
    private final JsonNode variables;
    private final GraphQLSchema schema;

    public FuzzErrorOrigins(FuzzInput input, GraphQLSchema schema) {
        variables = input.variables;
        this.schema = schema;
        object(schema.getQueryType(), input.root, input.selections, List.of());
    }

    public void check(JsonNode error) {
        String expected = origins.get(error.path("path").toString());
        String actual = error.path("extensions").path("classification").asText();
        if (!actual.equals(expected)) throw new DifferentialMismatch("Error has no matching primitive origin: " + error);
    }

    private void value(GraphQLType type, JsonNode data, JsonNode selections, List<Object> path, boolean resolver) {
        if (resolver && data.path("$error").asBoolean()) { origins.put(key(path), "DataFetchingException"); return; }
        if (type instanceof GraphQLNonNull && (data.isNull() || data.isMissingNode())) { origins.put(key(path), "NullValueInNonNullableField"); return; }
        if (data.isNull() || data.isMissingNode()) return;
        GraphQLType unwrapped = type instanceof GraphQLNonNull ? ((GraphQLNonNull) type).getWrappedType() : type;
        if (unwrapped instanceof GraphQLList) { list((GraphQLList) unwrapped, data, selections, path); return; }
        if (unwrapped instanceof GraphQLObjectType) object((GraphQLObjectType) unwrapped, data, selections, path);
        if (unwrapped instanceof GraphQLInterfaceType || unwrapped instanceof GraphQLUnionType) {
            object(schema.getObjectType(data.path("__typename").asText()), data, selections, path);
        }
    }

    private void list(GraphQLList type, JsonNode data, JsonNode selections, List<Object> path) {
        for (int index = 0; index < data.size(); index++) value(type.getWrappedType(), data.get(index), selections, append(path, index), false);
    }

    private void object(GraphQLObjectType type, JsonNode data, JsonNode selections, List<Object> path) {
        Map<String, ArrayNode> fields = new LinkedHashMap<>();
        collect(type, selections, fields);
        fields.forEach((alias, nodes) -> field(type, data, alias, nodes, path));
    }

    private void field(GraphQLObjectType type, JsonNode data, String alias, ArrayNode nodes, List<Object> path) {
        String name = nodes.get(0).path("name").asText();
        ArrayNode children = JsonNodeFactory.instance.arrayNode();
        nodes.forEach(node -> node.path("selections").forEach(children::add));
        value(type.getFieldDefinition(name).getType(), data.path(name), children, append(path, alias), true);
    }

    private void collect(GraphQLObjectType type, JsonNode nodes, Map<String, ArrayNode> fields) {
        for (JsonNode node : nodes) collectNode(type, node, fields);
    }

    private void collectNode(GraphQLObjectType type, JsonNode node, Map<String, ArrayNode> fields) {
        if (!included(node)) return;
        if (node.path("kind").asText().equals("fragment")) {
            if (matches(type, node)) collect(type, node.path("selections"), fields);
            return;
        }
        fields.computeIfAbsent(node.path("responseName").asText(), ignored -> JsonNodeFactory.instance.arrayNode()).add(node);
    }

    private boolean matches(GraphQLObjectType type, JsonNode fragment) {
        if (!fragment.has("typeCondition")) return true;
        GraphQLNamedType condition = (GraphQLNamedType) schema.getType(fragment.path("typeCondition").asText());
        if (condition instanceof GraphQLObjectType) return condition.equals(type);
        return schema.isPossibleType(condition, type);
    }

    private boolean included(JsonNode node) {
        for (JsonNode directive : node.path("directives")) if (excluded(directive)) return false;
        return true;
    }

    private boolean excluded(JsonNode directive) {
        JsonNode condition = directive.path("if");
        boolean enabled = condition.isObject() ? variables.path(condition.path("variable").asText()).asBoolean() : condition.asBoolean();
        String name = directive.path("name").asText();
        return name.equals("skip") && enabled || name.equals("include") && !enabled;
    }

    private List<Object> append(List<Object> path, Object segment) {
        List<Object> result = new ArrayList<>(path);
        result.add(segment);
        return result;
    }

    private String key(List<Object> path) {
        ArrayNode result = JsonNodeFactory.instance.arrayNode();
        path.forEach(segment -> { if (segment instanceof Integer) result.add((Integer) segment); else result.add((String) segment); });
        return result.toString();
    }
}
