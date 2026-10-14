package graphql.execution.incremental.v2;

import graphql.Internal;

import java.util.ArrayList;
import java.util.List;

@Internal
public final class IntegratedWork {
    public final List<DeliveryGroup> groups = new ArrayList<>();
    public final List<ItemStream> streams = new ArrayList<>();
    public final List<DeferredTask> activeTasks = new ArrayList<>();
    public final List<DeferredTask> rejectedTasks = new ArrayList<>();
}
