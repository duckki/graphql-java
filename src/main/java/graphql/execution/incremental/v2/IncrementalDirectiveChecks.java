package graphql.execution.incremental.v2;

import graphql.Directives;
import graphql.ExperimentalApi;
import graphql.GraphqlErrorException;
import graphql.Internal;
import graphql.execution.CoercedVariables;
import graphql.execution.conditional.ConditionalNodes;
import graphql.execution.ValuesResolver;
import graphql.language.Directive;
import graphql.language.DirectivesContainer;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.NodeUtil;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.ExecutionInput;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLSchema;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Internal
public final class IncrementalDirectiveChecks {
    private final ConditionalNodes conditions = new ConditionalNodes();

    public void checkSubscription(OperationDefinition operation, Map<String, FragmentDefinition> fragments,
                                   CoercedVariables variables, GraphQLSchema schema, ExecutionInput input) {
        if (operation.getOperation() != OperationDefinition.Operation.SUBSCRIPTION
                || !input.getGraphQLContext().getBoolean(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT_V2)) return;
        Deque<Selection> stack = new ArrayDeque<>(operation.getSelectionSet().getSelections());
        Set<String> visited = new LinkedHashSet<>();
        while (!stack.isEmpty()) checkSelection(stack.pop(), stack, visited, fragments, variables, schema, input);
    }

    private void checkSelection(Selection selection, Deque<Selection> stack, Set<String> visited,
                                 Map<String, FragmentDefinition> fragments, CoercedVariables variables,
                                 GraphQLSchema schema, ExecutionInput input) {
        DirectivesContainer<?> node = (DirectivesContainer<?>) selection;
        if (!conditions.shouldInclude(node, variables.toMap(), schema, input.getGraphQLContext())) return;
        checkActive(node.getDirectives(), variables, input);
        if (selection instanceof Field) { addChildren(stack, ((Field) selection).getSelectionSet()); return; }
        if (selection instanceof InlineFragment) { addChildren(stack, ((InlineFragment) selection).getSelectionSet()); return; }
        FragmentSpread spread = (FragmentSpread) selection;
        if (!visited.add(spread.getName())) return;
        FragmentDefinition fragment = fragments.get(spread.getName());
        if (fragment != null) addChildren(stack, fragment.getSelectionSet());
    }

    private void addChildren(Deque<Selection> stack, SelectionSet children) {
        if (children != null) stack.addAll(children.getSelections());
    }

    private void checkActive(List<Directive> directives, CoercedVariables variables, ExecutionInput input) {
        for (GraphQLDirective definition : List.of(Directives.DeferDirective, Directives.StreamDirective)) {
            Directive directive = NodeUtil.findNodeByName(directives, definition.getName());
            if (directive != null) rejectActive(definition, directive, variables, input);
        }
    }

    private void rejectActive(GraphQLDirective definition, Directive directive, CoercedVariables variables, ExecutionInput input) {
        Map<String, Object> arguments = ValuesResolver.getArgumentValues(definition.getArguments(), directive.getArguments(),
                variables, input.getGraphQLContext(), input.getLocale());
        if (Boolean.FALSE.equals(arguments.get("if"))) return;
        throw GraphqlErrorException.newErrorException().message("The @" + definition.getName() + " directive must be disabled in subscription operations")
                .sourceLocation(directive.getSourceLocation()).build();
    }
}
