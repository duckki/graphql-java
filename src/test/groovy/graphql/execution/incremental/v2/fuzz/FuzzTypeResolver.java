package graphql.execution.incremental.v2.fuzz;

import graphql.Internal;
import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

import java.util.Map;

@Internal
public final class FuzzTypeResolver implements TypeResolver {
    @Override
    public GraphQLObjectType getType(TypeResolutionEnvironment environment) {
        Map<?, ?> object = environment.getObject();
        return environment.getSchema().getObjectType(object.get("__typename").toString());
    }
}
