package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.ResultPath;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

@Internal
public final class IncrementalExecutionParameters {
    private final IncrementalWork work;
    private final CollectedFields collectedFields;
    private final Set<DeferUsage> parentUsages;
    private final DeferContext deferGroups;

    public IncrementalExecutionParameters(IncrementalWork work, CollectedFields collectedFields,
                                          Set<DeferUsage> parentUsages, DeferContext deferGroups) {
        this.work = graphql.Assert.assertNotNull(work);
        this.collectedFields = graphql.Assert.assertNotNull(collectedFields);
        this.parentUsages = Collections.unmodifiableSet(new LinkedHashSet<>(parentUsages));
        this.deferGroups = graphql.Assert.assertNotNull(deferGroups);
    }

    public IncrementalExecutionParameters withFields(CollectedFields fields, ResultPath path) {
        Map<DeferUsage, DeliveryGroup> groups = new LinkedHashMap<>();
        for (DeferUsage usage : fields.getNewDeferUsages()) {
            DeliveryGroup group = new DeliveryGroup(groups.containsKey(usage.getParent()) ? groups.get(usage.getParent()) : deferGroups.get(usage.getParent()), path, usage.getLabel(), usage.hasLabel());
            groups.put(usage, group);
            work.addGroup(group);
        }
        return new IncrementalExecutionParameters(work, fields, parentUsages, groups.isEmpty() ? deferGroups : new DeferContext(deferGroups, groups));
    }

    public IncrementalWork getWork() {
        return work;
    }
    public CollectedFields getCollectedFields() {
        return collectedFields;
    }
    public Set<DeferUsage> getParentUsages() {
        return parentUsages;
    }
    public DeferContext getDeferGroups() {
        return deferGroups;
    }

    public static IncrementalExecutionParametersBuilder newIncrementalExecutionParameters() {
        return new IncrementalExecutionParametersBuilder();
    }

    public IncrementalExecutionParameters transform(java.util.function.Consumer<IncrementalExecutionParametersBuilder> consumer) {
        IncrementalExecutionParametersBuilder builder = newIncrementalExecutionParameters().work(work).collectedFields(collectedFields).parentUsages(parentUsages).deferGroups(deferGroups);
        consumer.accept(builder);
        return builder.build();
    }
}
