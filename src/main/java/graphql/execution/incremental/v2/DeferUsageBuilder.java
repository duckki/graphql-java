package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.language.Directive;
import org.jspecify.annotations.NullUnmarked;


@Internal
@NullUnmarked
public final class DeferUsageBuilder {
    private DeferUsage parent = null;
    private String label = null;
    private Directive directive;

    public DeferUsageBuilder parent(DeferUsage parent) {
        this.parent = parent;
        return this;
    }

    public DeferUsageBuilder label(String label) {
        this.label = label;
        return this;
    }

    public DeferUsageBuilder directive(Directive directive) {
        this.directive = directive;
        return this;
    }

    public DeferUsage build() {
        return new DeferUsage(parent, label, directive);
    }
}
