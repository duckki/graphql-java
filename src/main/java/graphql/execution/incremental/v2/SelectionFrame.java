package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.language.Selection;
import graphql.language.Directive;
import org.jspecify.annotations.Nullable;

import java.util.Set;
import java.util.Map;

@Internal
public final class SelectionFrame {
    private final Selection<?> selection;
    private final @Nullable DeferUsage usage;
    private final Map<String, Set<Directive>> visitedFragments;

    public SelectionFrame(Selection<?> selection, @Nullable DeferUsage usage, Map<String, Set<Directive>> visitedFragments) {
        this.selection = selection;
        this.usage = usage;
        this.visitedFragments = visitedFragments;
    }

    public Selection<?> getSelection() {
        return selection;
    }
    public @Nullable DeferUsage getUsage() {
        return usage;
    }
    public Map<String, Set<Directive>> getVisitedFragments() {
        return visitedFragments;
    }
}
