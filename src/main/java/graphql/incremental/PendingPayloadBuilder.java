package graphql.incremental;

import graphql.PublicApi;
import org.jspecify.annotations.NullUnmarked;

import java.util.Collections;
import java.util.List;

/** Builder for {@link PendingPayload}. */
@PublicApi
@NullUnmarked
public final class PendingPayloadBuilder {
    private String id;
    private List<Object> path = Collections.emptyList();
    private String label;
    private boolean labelPresent;

    /**
     * @param id delivery ID
     * @return this builder
     */
    public PendingPayloadBuilder id(String id) {
        this.id = id;
        return this;
    }

    /**
     * @param path notice path
     * @return this builder
     */
    public PendingPayloadBuilder path(List<Object> path) {
        this.path = path;
        return this;
    }

    /**
     * @param label directive label
     * @return this builder
     */
    public PendingPayloadBuilder label(String label) {
        this.label = label;
        this.labelPresent = true;
        return this;
    }

    /**
     * @param present whether the label argument was supplied
     * @return this builder
     */
    public PendingPayloadBuilder labelPresent(boolean present) {
        this.labelPresent = present;
        return this;
    }

    /** @return the immutable notice */
    public PendingPayload build() {
        return new PendingPayload(id, path, label, labelPresent);
    }
}
