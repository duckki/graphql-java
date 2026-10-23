package graphql.execution.incremental.v2;

import graphql.Internal;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Internal
public final class GroupRecord {
    public final List<DeliveryGroup> children = new ArrayList<>();
    public final Set<DeferredTask> tasks = new LinkedHashSet<>();
    public int pending;
}
