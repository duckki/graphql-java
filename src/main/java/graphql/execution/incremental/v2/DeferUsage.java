package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.language.Directive;
import org.jspecify.annotations.Nullable;

@Internal
public final class DeferUsage {
    private final @Nullable DeferUsage parent;
    private final @Nullable String label;
    private final @Nullable Directive directive;

    public DeferUsage(@Nullable DeferUsage parent, @Nullable String label, @Nullable Directive directive) {
        this.parent = parent;
        this.label = label;
        this.directive = directive;
    }

    public @Nullable Directive getDirective() { return directive; }

    public @Nullable DeferUsage getParent() {
        return parent;
    }
    public @Nullable String getLabel() {
        return label;
    }

    public boolean hasLabel() {
        return label != null || directive != null && directive.getArgument("label") != null;
    }

    public static DeferUsageBuilder newDeferUsage() {
        return new DeferUsageBuilder();
    }

    public DeferUsage transform(java.util.function.Consumer<DeferUsageBuilder> consumer) {
        DeferUsageBuilder builder = newDeferUsage().parent(parent).label(label).directive(directive);
        consumer.accept(builder);
        return builder.build();
    }
}
