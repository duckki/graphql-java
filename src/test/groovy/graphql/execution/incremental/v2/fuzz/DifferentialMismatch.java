package graphql.execution.incremental.v2.fuzz;

import graphql.Internal;

@Internal
public final class DifferentialMismatch extends IllegalStateException {
    public DifferentialMismatch(String message) {
        super(message);
    }
}
