package graphql.execution.incremental.v2.fuzz;

import graphql.Internal;

@Internal
@FunctionalInterface
public interface FuzzInputCheck {
    void check(FuzzInput input) throws Exception;
}
