package graphql.execution.incremental.v2;

import graphql.Internal;
import org.jspecify.annotations.NullUnmarked;

import java.util.Collections;
import java.util.Map;

@Internal
@NullUnmarked
public final class DeferContextBuilder {
    private DeferContext parent = null;
    private Map<DeferUsage, DeliveryGroup> groups = Collections.emptyMap();

    public DeferContextBuilder parent(DeferContext parent) {
        this.parent = parent;
        return this;
    }

    public DeferContextBuilder groups(Map<DeferUsage, DeliveryGroup> groups) {
        this.groups = groups;
        return this;
    }

    public DeferContext build() {
        return new DeferContext(parent, groups);
    }
}
