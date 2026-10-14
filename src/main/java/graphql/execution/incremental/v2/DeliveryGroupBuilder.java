package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.ResultPath;
import org.jspecify.annotations.NullUnmarked;


@Internal
@NullUnmarked
public final class DeliveryGroupBuilder {
    private DeliveryGroup parent = null;
    private ResultPath path = null;
    private String label = null;
    private boolean labelPresent;

    public DeliveryGroupBuilder parent(DeliveryGroup parent) {
        this.parent = parent;
        return this;
    }

    public DeliveryGroupBuilder path(ResultPath path) {
        this.path = path;
        return this;
    }

    public DeliveryGroupBuilder label(String label) {
        this.label = label;
        this.labelPresent = true;
        return this;
    }

    public DeliveryGroupBuilder labelPresent(boolean present) {
        this.labelPresent = present;
        return this;
    }

    public DeliveryGroup build() {
        return new DeliveryGroup(parent, path, label, labelPresent);
    }
}
