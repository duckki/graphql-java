package graphql.incremental;

import graphql.ExperimentalApi;
import graphql.PublicApi;
import graphql.Internal;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;

/** A pending delivery ID and its response path. */
@PublicApi
@ExperimentalApi
@NullMarked
public final class PendingPayload {
    private final String id;
    private final List<Object> path;
    private final @Nullable String label;
    private final boolean labelPresent;

    @Internal
    public PendingPayload(String id, List<Object> path, @Nullable String label, boolean labelPresent) {
        this.id = assertNotNull(id);
        this.path = List.copyOf(assertNotNull(path));
        this.label = label;
        this.labelPresent = labelPresent;
    }

    /** @return the request-local delivery ID */
    public String getId() {
        return id;
    }

    /** @return the response path */
    public List<Object> getPath() {
        return path;
    }

    /** @return the directive label, if present */
    public @Nullable String getLabel() {
        return label;
    }

    /** @return whether the directive supplied a label argument, including null */
    public boolean hasLabel() {
        return labelPresent;
    }

    /** @return the serialized notice */
    public Map<String, @Nullable Object> toSpecification() {
        Map<String, @Nullable Object> result = new LinkedHashMap<>();
        result.put("id", id);
        result.put("path", path);
        if (labelPresent) result.put("label", label);
        return result;
    }

    /** @return a builder for a notice */
    public static PendingPayloadBuilder newPendingPayload() {
        return new PendingPayloadBuilder();
    }

    /** @return a copy with the requested changes */
    public PendingPayload transform(Consumer<PendingPayloadBuilder> consumer) {
        PendingPayloadBuilder builder = newPendingPayload().id(id).path(path).label(label).labelPresent(labelPresent);
        consumer.accept(builder);
        return builder.build();
    }
}
