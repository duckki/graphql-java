package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Directives;
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
import graphql.language.BooleanValue;
import graphql.language.IntValue;
import graphql.language.Directive;
import graphql.language.VariableReference;
import graphql.language.Value;
import graphql.language.NodeUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private final ScheduledExecutorService scheduler;
    public final GraphQL graphQL;

    public FuzzSchema(String mode) {
        if (!List.of("sync", "promise", "items", "async").contains(mode)) throw new IllegalArgumentException("Unknown resolver mode " + mode);
        scheduler = mode.equals("async") ? Executors.newScheduledThreadPool(2) : null;
        DataFetcher<Object> resolver = environment -> fetch(environment, mode);
        RuntimeWiring wiring = RuntimeWiring.newRuntimeWiring()
                .type(TypeRuntimeWiring.newTypeWiring("Query").defaultDataFetcher(resolver))
                .type(TypeRuntimeWiring.newTypeWiring("Item").defaultDataFetcher(resolver))
                .type(TypeRuntimeWiring.newTypeWiring("Other").defaultDataFetcher(resolver))
                .type(TypeRuntimeWiring.newTypeWiring("Node").typeResolver(new FuzzTypeResolver()))
                .type(TypeRuntimeWiring.newTypeWiring("Named").typeResolver(new FuzzTypeResolver()))
                .type(TypeRuntimeWiring.newTypeWiring("Search").typeResolver(new FuzzTypeResolver())).build();
        GraphQLSchema generated = new SchemaGenerator().makeExecutableSchema(new SchemaParser().parse(EXTENDED_SDL), wiring);
        schema = GraphQLSchema.newSchema(generated).additionalDirective(Directives.StreamDirective).build();
        graphQL = GraphQL.newGraphQL(schema).build();
    }

    private Object fetch(DataFetchingEnvironment environment, String mode) {
        Map<?, ?> source = environment.getSource();
        Object value = source.get(environment.getField().getName());
        boolean failure = resolverFailure(value);
        if (mode.equals("sync") && failure) throw new GraphQLException("generated resolver failure");
        if ((mode.equals("items") || mode.equals("async")) && value instanceof List && streamed(environment)) value = futureItems((List<?>) value, environment);
        if (value instanceof List && streamed(environment)) value = tracked((List<?>) value, environment);
        if (mode.equals("sync")) return value;
        return future(value, failure, environment);
    }

    private List<Object> futureItems(List<?> values, DataFetchingEnvironment environment) {
        List<Object> result = new ArrayList<>();
        for (Object value : values) result.add(future(value, resolverFailure(value), environment));
        return result;
    }

    private CompletableFuture<Object> future(Object value, boolean failure, DataFetchingEnvironment environment) {
        if (scheduler == null) return failure ? CompletableFuture.failedFuture(new GraphQLException("generated resolver failure")) : CompletableFuture.completedFuture(value);
        CompletableFuture<Object> result = new CompletableFuture<>();
        long seed = environment.getGraphQlContext().getOrDefault("FUZZ_SEED", 0L);
        long delay = 1 + Math.floorMod(seed + environment.getExecutionStepInfo().getPath().toString().hashCode(), 3);
        scheduler.schedule(() -> settle(result, value, failure), delay, TimeUnit.MILLISECONDS);
        return result;
    }

    private void settle(CompletableFuture<Object> result, Object value, boolean failure) {
        if (failure) { result.completeExceptionally(new GraphQLException("generated resolver failure")); return; }
        result.complete(value);
    }

    private boolean resolverFailure(Object value) {
        return value instanceof Map && Boolean.TRUE.equals(((Map<?, ?>) value).get("$error"));
    }

    private boolean streamed(DataFetchingEnvironment environment) {
        Directive directive = NodeUtil.findNodeByName(environment.getField().getDirectives(), "stream");
        if (directive == null || directive.getArgument("if") == null) return directive != null;
        Value<?> condition = directive.getArgument("if").getValue();
        if (condition instanceof BooleanValue) return ((BooleanValue) condition).isValue();
        return !Boolean.FALSE.equals(environment.getVariables().get(((VariableReference) condition).getName()));
    }

    private Object tracked(List<?> values, DataFetchingEnvironment environment) {
        FuzzHostState state = environment.getGraphQlContext().get("FUZZ_HOST");
        if (state == null) return values;
        Directive directive = NodeUtil.findNodeByName(environment.getField().getDirectives(), "stream");
        Value<?> count = directive.getArgument("initialCount").getValue();
        int prefix = count instanceof IntValue ? ((IntValue) count).getValue().intValue() : ((Number) environment.getVariables().get(((VariableReference) count).getName())).intValue();
        FuzzTrackedIterable source = new FuzzTrackedIterable(values, environment.getExecutionStepInfo().getPath().toList(), prefix);
        state.sources.add(source);
        return source;
    }

    public GraphQLSchema getSchema() { return schema; }

    @Override
    public void close() { if (scheduler != null) scheduler.shutdownNow(); }

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
