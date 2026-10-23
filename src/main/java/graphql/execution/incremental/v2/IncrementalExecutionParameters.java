package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.ResultPath;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
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
    private final List<DeliveryGroup> declarations;

    public IncrementalExecutionParameters(IncrementalWork work, CollectedFields collectedFields,
                                          Set<DeferUsage> parentUsages, DeferContext deferGroups) {
        this(work, collectedFields, parentUsages, deferGroups, Collections.emptyList());
    }

    public IncrementalExecutionParameters(IncrementalWork work, CollectedFields collectedFields,
                                          Set<DeferUsage> parentUsages, DeferContext deferGroups, List<DeliveryGroup> declarations) {
        this.work = graphql.Assert.assertNotNull(work);
        this.collectedFields = graphql.Assert.assertNotNull(collectedFields);
        this.parentUsages = Collections.unmodifiableSet(new LinkedHashSet<>(parentUsages));
        this.deferGroups = graphql.Assert.assertNotNull(deferGroups);
        this.declarations = List.copyOf(declarations);
    }

    public IncrementalExecutionParameters withFields(CollectedFields fields, ResultPath path) {
        Map<DeferUsage, DeliveryGroup> groups = new LinkedHashMap<>();
        for (DeferUsage usage : fields.getNewDeferUsages()) {
            DeliveryGroup group = new DeliveryGroup(groups.containsKey(usage.getParent()) ? groups.get(usage.getParent()) : deferGroups.get(usage.getParent()), path, usage.getLabel(), usage.hasLabel());
            groups.put(usage, group);
        }
        return new IncrementalExecutionParameters(work, fields, parentUsages, groups.isEmpty() ? deferGroups : new DeferContext(deferGroups, groups), new ArrayList<>(groups.values()));
    }

    public void declareGroups() {
        declarations.forEach(work::addGroup);
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
        IncrementalExecutionParametersBuilder builder = newIncrementalExecutionParameters().work(work).collectedFields(collectedFields).parentUsages(parentUsages).deferGroups(deferGroups).declarations(declarations);
        consumer.accept(builder);
        return builder.build();
    }
}
