package graphql.incremental;

import graphql.GraphQLError;
import graphql.PublicApi;
import org.jspecify.annotations.NullUnmarked;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Builder for {@link IncrementalPayloadV2}. */
@PublicApi
@NullUnmarked
public final class IncrementalPayloadV2Builder {
    private String id;
    private List<Object> subPath = Collections.emptyList();
    private Map<String, Object> data;
    private List<GraphQLError> errors = Collections.emptyList();

    /**
     * @param id pending delivery ID
     * @return this builder
     */
    public IncrementalPayloadV2Builder id(String id) {
        this.id = id;
        return this;
    }
    /**
     * @param subPath relative response path
     * @return this builder
     */
    public IncrementalPayloadV2Builder subPath(List<Object> subPath) {
        this.subPath = subPath;
        return this;
    }
    /**
     * @param data deferred fields
     * @return this builder
     */
    public IncrementalPayloadV2Builder data(Map<String, Object> data) {
        this.data = data;
        return this;
    }
    /**
     * @param errors patch errors
     * @return this builder
     */
    public IncrementalPayloadV2Builder errors(List<GraphQLError> errors) {
        this.errors = errors;
        return this;
    }
    /** @return the immutable patch */
    public IncrementalPayloadV2 build() {
        return new IncrementalPayloadV2(id, subPath, data, errors);
    }
}
