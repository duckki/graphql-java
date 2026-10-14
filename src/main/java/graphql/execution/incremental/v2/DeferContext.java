package graphql.execution.incremental.v2;

import graphql.Internal;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@Internal
public final class DeferContext {
    private final @Nullable DeferContext parent;
    private final Map<DeferUsage, DeliveryGroup> groups;

    public DeferContext(@Nullable DeferContext parent, Map<DeferUsage, DeliveryGroup> groups) {
        this.parent = parent;
        this.groups = Collections.unmodifiableMap(new LinkedHashMap<>(groups));
    }

    public @Nullable DeliveryGroup get(@Nullable DeferUsage usage) {
        DeferContext current = this;
        while (current != null) {
            DeliveryGroup group = current.groups.get(usage);
            if (group != null) return group;
            current = current.parent;
        }
        return null;
    }

    public static DeferContextBuilder newDeferContext() {
        return new DeferContextBuilder();
    }

    public DeferContext transform(java.util.function.Consumer<DeferContextBuilder> consumer) {
        DeferContextBuilder builder = newDeferContext().parent(parent).groups(groups);
        consumer.accept(builder);
        return builder.build();
    }
}
