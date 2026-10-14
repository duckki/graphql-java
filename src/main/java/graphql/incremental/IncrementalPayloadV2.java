package graphql.incremental;

import graphql.ExperimentalApi;
import graphql.GraphQLError;
import graphql.PublicApi;
import graphql.Internal;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertTrue;

/** An ID-based defer or stream patch in the incremental delivery draft. */
@PublicApi
@ExperimentalApi
@NullMarked
public final class IncrementalPayloadV2 extends IncrementalPayload {
    private final String id;
    private final List<Object> subPath;
    private final @Nullable Map<String, @Nullable Object> data;
    private final @Nullable List<@Nullable Object> items;
    private final List<GraphQLError> errors;

    @Internal
    public IncrementalPayloadV2(String id, List<Object> subPath, @Nullable Map<String, @Nullable Object> data,
                                @Nullable List<@Nullable Object> items, List<GraphQLError> errors) {
        super(Collections.emptyList(), null, List.copyOf(errors), null);
        assertTrue((data == null) != (items == null), "A patch must contain either data or items");
        this.id = assertNotNull(id);
        this.subPath = List.copyOf(subPath);
        this.data = data == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(data));
        this.items = items == null ? null : Collections.unmodifiableList(new ArrayList<>(items));
        this.errors = List.copyOf(errors);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public List<Object> getSubPath() {
        return subPath;
    }

    /** @return the deferred object fields, or null for a stream patch */
    public @Nullable Map<String, @Nullable Object> getData() {
        return data;
    }

    /** @return the streamed items, or null for a defer patch */
    public @Nullable List<@Nullable Object> getItems() {
        return items;
    }

    @Override
    public List<GraphQLError> getErrors() {
        return errors;
    }

    @Override
    public Map<String, Object> toSpecification() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", id);
        if (!subPath.isEmpty()) result.put("subPath", subPath);
        if (data != null) result.put("data", data);
        if (items != null) result.put("items", items);
        if (!errors.isEmpty()) result.put("errors", errors.stream().map(GraphQLError::toSpecification).collect(Collectors.toList()));
        return result;
    }

    /** @return a builder for a patch */
    public static IncrementalPayloadV2Builder newIncrementalPayloadV2() {
        return new IncrementalPayloadV2Builder();
    }

    /** @return a copy with the requested changes */
    public IncrementalPayloadV2 transform(Consumer<IncrementalPayloadV2Builder> consumer) {
        IncrementalPayloadV2Builder builder = newIncrementalPayloadV2().id(id).subPath(subPath).data(data).items(items).errors(errors);
        consumer.accept(builder);
        return builder.build();
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof IncrementalPayloadV2 && toSpecification().equals(((IncrementalPayloadV2) other).toSpecification());
    }

    @Override
    public int hashCode() {
        return toSpecification().hashCode();
    }
}
