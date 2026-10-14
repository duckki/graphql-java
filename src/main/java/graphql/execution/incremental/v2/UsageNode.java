package graphql.execution.incremental.v2;

import graphql.Internal;

import java.util.ArrayList;
import java.util.List;

@Internal
public final class UsageNode {
    public final List<UsageNode> children = new ArrayList<>();
    public int start;
    public int end;
    public int nextChild;
}
