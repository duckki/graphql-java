package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.FieldCollectorParameters;
import graphql.execution.TypeFromAST;
import graphql.execution.conditional.ConditionalNodes;
import graphql.execution.incremental.IncrementalUtils;
import graphql.language.Field;
import graphql.language.DirectivesContainer;
import graphql.language.Directive;
import graphql.language.NodeUtil;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.language.TypeName;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLUnionType;
import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.IdentityHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.Set;

@Internal
public final class IncrementalFieldCollector {
    private final ConditionalNodes conditionalNodes = new ConditionalNodes();

    public CollectedFields collectFields(FieldCollectorParameters parameters, SelectionSet selectionSet) {
        CollectedFields result = new CollectedFields();
        collect(parameters, selectionSet, null, new LinkedHashMap<>(), result);
        return result;
    }

    public CollectedFields collectSubfields(FieldCollectorParameters parameters, List<FieldDetails> fields) {
        CollectedFields result = new CollectedFields();
        for (FieldDetails details : fields) {
            SelectionSet selectionSet = details.getField().getSelectionSet();
            if (selectionSet != null) collect(parameters, selectionSet, details.getDeferUsage(), new LinkedHashMap<>(), result);
        }
        return result;
    }

    private void collect(FieldCollectorParameters parameters, SelectionSet selectionSet, @Nullable DeferUsage usage,
                         Map<String, Set<Directive>> visited, CollectedFields result) {
        Deque<SelectionFrame> stack = new ArrayDeque<>();
        pushSelections(stack, selectionSet, usage, visited);
        while (!stack.isEmpty()) collectSelection(parameters, stack.pop(), stack, result);
    }

    private void pushSelections(Deque<SelectionFrame> stack, SelectionSet selections, @Nullable DeferUsage usage, Map<String, Set<Directive>> visited) {
        List<Selection> nodes = selections.getSelections();
        for (int index = nodes.size() - 1; index >= 0; index--) {
            stack.push(new SelectionFrame(nodes.get(index), usage, visited));
        }
    }

    private void collectSelection(FieldCollectorParameters parameters, SelectionFrame frame, Deque<SelectionFrame> stack, CollectedFields result) {
        Selection<?> selection = frame.getSelection();
        if (!conditionalNodes.shouldInclude((DirectivesContainer<?>) selection, parameters.getVariables(), parameters.getGraphQLSchema(), parameters.getGraphQLContext())) return;
        if (selection instanceof Field) {
            result.addField(new FieldDetails((Field) selection, frame.getUsage()));
            return;
        }
        if (selection instanceof InlineFragment) {
            collectInline(parameters, (InlineFragment) selection, frame, stack, result);
            return;
        }
        collectSpread(parameters, (FragmentSpread) selection, frame, stack, result);
    }

    private void collectInline(FieldCollectorParameters parameters, InlineFragment fragment, SelectionFrame frame,
                               Deque<SelectionFrame> stack, CollectedFields result) {
        if (!matches(parameters, fragment.getTypeCondition())) return;
        DeferUsage usage = IncrementalUtils.createDeferredExecution(parameters.getVariables(), fragment.getDirectives(),
                label -> new DeferUsage(frame.getUsage(), label, NodeUtil.findNodeByName(fragment.getDirectives(), "defer")));
        pushFragment(stack, fragment.getSelectionSet(), frame, usage, result);
    }

    private void collectSpread(FieldCollectorParameters parameters, FragmentSpread spread, SelectionFrame frame,
                               Deque<SelectionFrame> stack, CollectedFields result) {
        FragmentDefinition fragment = parameters.getFragmentsByName().get(spread.getName());
        if (fragment == null || !matches(parameters, fragment.getTypeCondition())) return;
        DeferUsage usage = IncrementalUtils.createDeferredExecution(parameters.getVariables(), spread.getDirectives(),
                label -> new DeferUsage(frame.getUsage(), label, NodeUtil.findNodeByName(spread.getDirectives(), "defer")));
        Set<Directive> visited = frame.getVisitedFragments().computeIfAbsent(spread.getName(),
                key -> Collections.newSetFromMap(new IdentityHashMap<>()));
        if (visited.contains(null)) return;
        DeferUsage context = usage == null ? frame.getUsage() : usage;
        if (!visited.add(context == null ? null : context.getDirective())) return;
        pushFragment(stack, fragment.getSelectionSet(), frame, usage, result);
    }

    private void pushFragment(Deque<SelectionFrame> stack, SelectionSet selectionSet, SelectionFrame frame,
                              @Nullable DeferUsage usage, CollectedFields result) {
        if (usage == null) {
            pushSelections(stack, selectionSet, frame.getUsage(), frame.getVisitedFragments());
            return;
        }
        result.addDeferUsage(usage);
        pushSelections(stack, selectionSet, usage, frame.getVisitedFragments());
    }

    private boolean matches(FieldCollectorParameters parameters, @Nullable TypeName typeName) {
        if (typeName == null) return true;
        GraphQLType condition = TypeFromAST.getTypeFromAST(parameters.getGraphQLSchema(), typeName);
        if (parameters.getObjectType().equals(condition)) return true;
        if (condition instanceof GraphQLInterfaceType) return parameters.getGraphQLSchema().getImplementations((GraphQLInterfaceType) condition).contains(parameters.getObjectType());
        return condition instanceof GraphQLUnionType && ((GraphQLUnionType) condition).getTypes().contains(parameters.getObjectType());
    }
}
