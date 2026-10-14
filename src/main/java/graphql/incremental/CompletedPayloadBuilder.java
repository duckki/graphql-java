package graphql.incremental;

import graphql.GraphQLError;
import graphql.PublicApi;
import org.jspecify.annotations.NullUnmarked;

import java.util.Collections;
import java.util.List;

/** Builder for {@link CompletedPayload}. */
@PublicApi
@NullUnmarked
public final class CompletedPayloadBuilder {
    private String id;
    private List<GraphQLError> errors = Collections.emptyList();

    /**
     * @param id delivery ID
     * @return this builder
     */
    public CompletedPayloadBuilder id(String id) {
        this.id = id;
        return this;
    }

    /**
     * @param errors notice errors
     * @return this builder
     */
    public CompletedPayloadBuilder errors(List<GraphQLError> errors) {
        this.errors = errors;
        return this;
    }

    /** @return the immutable notice */
    public CompletedPayload build() {
        return new CompletedPayload(id, errors);
    }
}
