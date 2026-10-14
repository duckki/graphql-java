package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.ResultPath;
import org.jspecify.annotations.Nullable;

@Internal
public final class DeliveryGroup {
    private final @Nullable DeliveryGroup parent;
    private final ResultPath path;
    private final @Nullable String label;
    private final boolean labelPresent;

    public DeliveryGroup(@Nullable DeliveryGroup parent, ResultPath path, @Nullable String label) {
        this(parent, path, label, label != null);
    }

    public DeliveryGroup(@Nullable DeliveryGroup parent, ResultPath path, @Nullable String label, boolean labelPresent) {
        this.parent = parent;
        this.path = graphql.Assert.assertNotNull(path);
        this.label = label;
        this.labelPresent = labelPresent;
    }

    public @Nullable DeliveryGroup getParent() {
        return parent;
    }
    public ResultPath getPath() {
        return path;
    }
    public @Nullable String getLabel() {
        return label;
    }

    public boolean hasLabel() { return labelPresent; }

    public static DeliveryGroupBuilder newDeliveryGroup() {
        return new DeliveryGroupBuilder();
    }

    public DeliveryGroup transform(java.util.function.Consumer<DeliveryGroupBuilder> consumer) {
        DeliveryGroupBuilder builder = newDeliveryGroup().parent(parent).path(path).label(label).labelPresent(labelPresent);
        consumer.accept(builder);
        return builder.build();
    }
}
