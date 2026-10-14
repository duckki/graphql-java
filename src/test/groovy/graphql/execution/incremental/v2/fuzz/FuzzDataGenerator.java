package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLUnionType;
import graphql.schema.GraphQLNamedType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SplittableRandom;

@Internal
public final class FuzzDataGenerator {
    private final JsonNodeFactory json = JsonNodeFactory.instance;
    private final SplittableRandom random;
    private final int listLength;
    private final boolean errors;
    private final boolean fatalErrors;
    private final GraphQLSchema schema;
    private int remainingItems = 192;

    public FuzzDataGenerator(GraphQLSchema schema, long seed, int listLength, boolean errors, boolean fatalErrors) {
        this.schema = schema;
        random = new SplittableRandom(seed ^ 0x51e3a728L);
        this.listLength = listLength;
        this.errors = errors;
        this.fatalErrors = fatalErrors;
    }

    public ObjectNode generate(GraphQLObjectType type, ArrayNode selections, String path) {
        ObjectNode object = json.objectNode().put("__typename", type.getName());
        Map<String, ArrayNode> fields = new LinkedHashMap<>();
        collect(type, selections, fields);
        fields.forEach((name, children) -> object.set(name, value(type.getFieldDefinition(name).getType(), children, path + "/" + name, true)));
        return object;
    }

    private void collect(GraphQLObjectType type, JsonNode selections, Map<String, ArrayNode> fields) {
        for (JsonNode node : selections) collectSelection(type, node, fields);
    }

    private void collectSelection(GraphQLObjectType type, JsonNode node, Map<String, ArrayNode> fields) {
        if (node.path("kind").asText().equals("fragment")) {
            if (matches(type, node)) collect(type, node.path("selections"), fields);
            return;
        }
        ArrayNode children = fields.computeIfAbsent(node.path("name").asText(), ignored -> json.arrayNode());
        node.path("selections").forEach(children::add);
    }

    private boolean matches(GraphQLObjectType type, JsonNode fragment) {
        if (!fragment.has("typeCondition")) return true;
        GraphQLNamedType condition = (GraphQLNamedType) schema.getType(fragment.path("typeCondition").asText());
        if (condition instanceof GraphQLObjectType) return condition.equals(type);
        return schema.isPossibleType(condition, type);
    }

    private JsonNode value(GraphQLType type, ArrayNode selections, String path, boolean resolver) {
        boolean nonNull = type instanceof GraphQLNonNull;
        boolean mayFail = !nonNull || fatalErrors;
        if (resolver && errors && mayFail && random.nextInt(9) == 0) return json.objectNode().put("$error", true);
        if ((!nonNull || errors && fatalErrors) && random.nextInt(12) == 0) return json.nullNode();
        GraphQLType unwrapped = nonNull ? ((GraphQLNonNull) type).getWrappedType() : type;
        if (unwrapped instanceof GraphQLList) return list((GraphQLList) unwrapped, selections, path);
        if (unwrapped instanceof GraphQLObjectType) return generate((GraphQLObjectType) unwrapped, selections, path);
        if (unwrapped instanceof GraphQLInterfaceType) {
            var types = schema.getImplementations((GraphQLInterfaceType) unwrapped);
            return generate(types.get(random.nextInt(types.size())), selections, path);
        }
        if (unwrapped instanceof GraphQLUnionType) {
            var types = ((GraphQLUnionType) unwrapped).getTypes();
            return generate(schema.getObjectType(types.get(random.nextInt(types.size())).getName()), selections, path);
        }
        return json.textNode(path + ":" + random.nextInt(100));
    }

    private ArrayNode list(GraphQLList type, ArrayNode selections, String path) {
        ArrayNode result = json.arrayNode();
        int count = Math.min(remainingItems, random.nextInt(listLength + 1));
        remainingItems -= count;
        for (int index = 0; index < count; index++) result.add(value(type.getWrappedType(), selections, path + "/" + index, false));
        return result;
    }
}
