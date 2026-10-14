package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.GraphQL;
import graphql.GraphQLException;
import graphql.Internal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLUnionType;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeRuntimeWiring;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Internal
public final class FuzzSchema implements AutoCloseable {
    public static final String SDL = "type Query { a: String b: String required: String! values: [String] strictValues: [String!] user: Item users: [Item] strictUsers: [Item!] } "
            + "type Item { id: String! name: String required: String! values: [String] strictValues: [String!] matrix: [[String]] strictMatrix: [[String!]!] friend: Item friends: [Item] }";
    public static final String EXTENDED_SDL = SDL + " extend type Query { matrix: [[String]] strictMatrix: [[String!]!] mandatoryUser: Item! mandatoryUsers: [Item!]! node: Named nodes: [Named] search: [Search] } "
            + "extend type Item implements Named & Node { itemOnly: String } "
            + "interface Node { id: String! } interface Named implements Node { id: String! name: String } "
            + "type Other implements Named & Node { id: String! name: String otherOnly: String } union Search = Item | Other";
    private final JsonNodeFactory json = JsonNodeFactory.instance;
    private final GraphQLSchema schema;
    public final GraphQL graphQL;

    public FuzzSchema(String mode) {
        if (!List.of("sync", "promise").contains(mode)) throw new IllegalArgumentException("Unknown resolver mode " + mode);
        DataFetcher<Object> resolver = environment -> fetch(environment, mode);
        RuntimeWiring wiring = RuntimeWiring.newRuntimeWiring()
                .type(TypeRuntimeWiring.newTypeWiring("Query").defaultDataFetcher(resolver))
                .type(TypeRuntimeWiring.newTypeWiring("Item").defaultDataFetcher(resolver))
                .type(TypeRuntimeWiring.newTypeWiring("Other").defaultDataFetcher(resolver))
                .type(TypeRuntimeWiring.newTypeWiring("Node").typeResolver(new FuzzTypeResolver()))
                .type(TypeRuntimeWiring.newTypeWiring("Named").typeResolver(new FuzzTypeResolver()))
                .type(TypeRuntimeWiring.newTypeWiring("Search").typeResolver(new FuzzTypeResolver())).build();
        GraphQLSchema generated = new SchemaGenerator().makeExecutableSchema(new SchemaParser().parse(EXTENDED_SDL), wiring);
        schema = generated;
        graphQL = GraphQL.newGraphQL(schema).build();
    }

    private Object fetch(DataFetchingEnvironment environment, String mode) {
        Map<?, ?> source = environment.getSource();
        Object value = source.get(environment.getField().getName());
        boolean failure = resolverFailure(value);
        if (mode.equals("sync") && failure) throw new GraphQLException("generated resolver failure");
        if (mode.equals("sync")) return value;
        return future(value, failure, environment);
    }

    private CompletableFuture<Object> future(Object value, boolean failure, DataFetchingEnvironment environment) {
        return failure ? CompletableFuture.failedFuture(new GraphQLException("generated resolver failure")) : CompletableFuture.completedFuture(value);
    }

    private boolean resolverFailure(Object value) {
        return value instanceof Map && Boolean.TRUE.equals(((Map<?, ?>) value).get("$error"));
    }

    public GraphQLSchema getSchema() { return schema; }

    @Override
    public void close() { }

    public ArrayNode modelSchema() {
        ArrayNode types = json.arrayNode();
        for (String name : List.of("Query", "Item", "Other", "Node", "Named", "Search")) types.add(modelType(name));
        return types;
    }

    private ObjectNode modelType(String name) {
        ObjectNode type = json.objectNode().put("name", name);
        GraphQLNamedType definition = (GraphQLNamedType) schema.getType(name);
        ArrayNode fields = json.arrayNode();
        if (definition instanceof GraphQLFieldsContainer) ((GraphQLFieldsContainer) definition).getFieldDefinitions().forEach(field -> fields.add(modelField(field)));
        type.put("kind", definition instanceof GraphQLInterfaceType ? "interface" : definition instanceof GraphQLUnionType ? "union" : "object");
        ArrayNode interfaces = json.arrayNode();
        if (definition instanceof GraphQLObjectType) ((GraphQLObjectType) definition).getInterfaces().forEach(parent -> interfaces.add(parent.getName()));
        if (definition instanceof GraphQLInterfaceType) ((GraphQLInterfaceType) definition).getInterfaces().forEach(parent -> interfaces.add(parent.getName()));
        type.set("interfaces", interfaces);
        if (definition instanceof GraphQLUnionType) {
            ArrayNode members = json.arrayNode();
            ((GraphQLUnionType) definition).getTypes().forEach(member -> members.add(member.getName()));
            type.set("members", members);
        }
        type.set("fields", fields);
        return type;
    }

    private JsonNode modelField(GraphQLFieldDefinition field) {
        ObjectNode result = json.objectNode().put("name", field.getName());
        result.set("type", typeRef(field.getType())); return result;
    }

    private JsonNode typeRef(GraphQLType type) {
        if (type instanceof GraphQLNonNull) return json.arrayNode().add("nonNull").add(typeRef(((GraphQLNonNull) type).getWrappedType()));
        if (type instanceof GraphQLList) return json.arrayNode().add("list").add(typeRef(((GraphQLList) type).getWrappedType()));
        return json.textNode(((GraphQLNamedType) type).getName());
    }
}
