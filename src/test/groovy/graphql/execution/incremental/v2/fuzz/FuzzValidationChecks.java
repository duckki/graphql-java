package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Directives;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.ExperimentalApi;
import graphql.GraphQL;
import graphql.Internal;
import graphql.execution.pubsub.ReactiveStreamsObjectPublisher;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeRuntimeWiring;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Internal
public final class FuzzValidationChecks {
    private final ObjectMapper json = new ObjectMapper();
    private final GraphQL graphQL;
    private final FuzzSemanticCoverage coverage;
    public ObjectNode observation;

    public FuzzValidationChecks(FuzzSemanticCoverage coverage) {
        this.coverage = coverage;
        String sdl = FuzzSchema.EXTENDED_SDL + " type Mutation { update: Item } type Subscription { event: Item }";
        RuntimeWiring wiring = RuntimeWiring.newRuntimeWiring()
                .type(TypeRuntimeWiring.newTypeWiring("Node").typeResolver(new FuzzTypeResolver()))
                .type(TypeRuntimeWiring.newTypeWiring("Named").typeResolver(new FuzzTypeResolver()))
                .type(TypeRuntimeWiring.newTypeWiring("Search").typeResolver(new FuzzTypeResolver())).build();
        GraphQLSchema generated = new SchemaGenerator().makeExecutableSchema(new SchemaParser().parse(sdl), wiring);
        graphQL = GraphQL.newGraphQL(GraphQLSchema.newSchema(generated).additionalDirective(Directives.StreamDirective).build()).build();
    }

    public void check(long seed) {
        observation = null;
        subscription(seed);
        invalid(seed);
    }

    private void subscription(long seed) {
        int choice = (int) (seed % 6);
        long flags = seed / 6;
        boolean enabled = (flags & 1) != 0;
        boolean included = (flags & 2) != 0;
        boolean skipped = (flags & 4) != 0;
        String query = "subscription($enabled: Boolean!, $included: Boolean!, $skipped: Boolean!) { event { id ... @include(if: $included) @skip(if: $skipped) { "
                + body(choice) + " } } }" + (choice == 2 || choice == 3 ? " fragment F on Item { ... @defer(if: $enabled) { name } }" : "");
        Map<String, Object> variables = Map.of("enabled", enabled, "included", included, "skipped", skipped);
        boolean forbidden = included && !skipped && (enabled || choice == 4);
        ExecutionResult result = execute(query, variables);
        observation = json.createObjectNode().put("seed", seed).put("query", query).put("forbidden", forbidden);
        observation.set("variables", json.valueToTree(variables));
        observation.put("publisher", result.getData() instanceof Publisher).put("dataPresent", result.isDataPresent());
        observation.set("errors", json.valueToTree(result.getErrors().stream().map(error -> error.toSpecification()).collect(Collectors.toList())));
        if (forbidden) {
            require(!result.getErrors().isEmpty() && result.getErrors().get(0).getMessage().contains("must be disabled"), "Active subscription delivery must be rejected after coercion");
            coverage.host("native.subscription-rejected");
            return;
        }
        require(result.getErrors().isEmpty() && result.getData() instanceof Publisher, "Disabled or skipped subscription delivery must stay ordinary");
        coverage.host("native.subscription-allowed");
    }

    private String body(int choice) {
        switch (choice) {
            case 0: return "... @defer(if: $enabled) { name }";
            case 1: return "values @stream(if: $enabled)";
            case 2: return "...F";
            case 3: return "...F ...F";
            case 4: return "... @defer(if: false) { ... @defer { name @include(if: $enabled) } }";
            default: return "... on Item { values @stream(if: $enabled, initialCount: 0) }";
        }
    }

    private void invalid(long seed) {
        List<String> queries = List.of("{ a @stream }", "{ values @stream(initialCount: -1) }",
                "query($label: String!) { ... @defer(label: $label) { a } }",
                "{ ... @defer(label: \"same\") { a } ... @defer(label: \"same\") { b } }",
                "{ values @stream values @stream }", "mutation { ... @defer { update { id } } }",
                "{ values @stream(initialCount: \"wrong\") }", "query { user { id ",
                "{ missing }", "{ values @stream(initialCount: null) }");
        String query = queries.get((int) (seed % queries.size()));
        ExecutionResult first = execute(query, Map.of("label", "label"));
        ExecutionResult second = execute(query, Map.of("label", "label"));
        observation = json.createObjectNode().put("seed", seed).put("query", query).put("expectedInvalid", true);
        observation.set("result", json.valueToTree(first.toSpecification()));
        require(!first.getErrors().isEmpty() && !first.isDataPresent(), "Invalid query must be rejected");
        require(first.getErrors().stream().map(error -> error.toSpecification()).collect(Collectors.toList())
                .equals(second.getErrors().stream().map(error -> error.toSpecification()).collect(Collectors.toList())), "Validation diagnostics must be stable");
        coverage.host("native.validation-negative." + seed % queries.size());
    }

    private ExecutionResult execute(String query, Map<String, Object> variables) {
        Object publisher = new ReactiveStreamsObjectPublisher(1, ignored -> Map.of("id", "item", "name", "name", "values", List.of("value")));
        return graphQL.execute(ExecutionInput.newExecutionInput().query(query).variables(variables).root(Map.of("event", publisher))
                .graphQLContext(Map.of(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2, true)).build());
    }

    private void require(boolean valid, String message) { if (!valid) throw new DifferentialMismatch(message); }
}
