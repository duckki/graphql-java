package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.ResultPath;
import graphql.execution.incremental.AlternativeCallContext;

@Internal
public final class StreamCallContext extends AlternativeCallContext {
    private final ResultPath itemPath;
    private int fieldCount;

    public StreamCallContext(ResultPath itemPath) {
        super(itemPath.getLevel() + 1, 0);
        this.itemPath = itemPath;
    }

    public void configure(ResultPath path, int fieldCount) {
        if (itemPath.equals(path)) this.fieldCount = fieldCount;
    }

    @Override
    public int getFields() {
        return fieldCount;
    }
}
