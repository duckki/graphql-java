package graphql.incremental;

import graphql.ExperimentalApi;
import graphql.GraphQLError;
import graphql.PublicApi;
import graphql.Internal;
import org.jspecify.annotations.NullMarked;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;

/** A notice that a delivery ID has completed, with any fatal errors. */
@PublicApi
@ExperimentalApi
@NullMarked
public final class CompletedPayload {
    private final String id;
    private final List<GraphQLError> errors;

    @Internal
    public CompletedPayload(String id, List<GraphQLError> errors) {
        this.id = assertNotNull(id);
        this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
    }

    /** @return the request-local delivery ID */
    public String getId() {
        return id;
    }

    /** @return the fatal errors, or an empty list on success */
    public List<GraphQLError> getErrors() {
        return errors;
    }

    /** @return the serialized notice */
    public Map<String, Object> toSpecification() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", id);
        if (!errors.isEmpty()) result.put("errors", errors.stream().map(GraphQLError::toSpecification).collect(java.util.stream.Collectors.toList()));
        return result;
    }

    /** @return a builder for a notice */
    public static CompletedPayloadBuilder newCompletedPayload() {
        return new CompletedPayloadBuilder();
    }

    /** @return a copy with the requested changes */
    public CompletedPayload transform(Consumer<CompletedPayloadBuilder> consumer) {
        CompletedPayloadBuilder builder = newCompletedPayload().id(id).errors(errors);
        consumer.accept(builder);
        return builder.build();
    }
}
