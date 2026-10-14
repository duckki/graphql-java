package graphql.execution.incremental.v2;

import graphql.Internal;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Internal
public final class ExecutionPlan {
    private final DeferUsageIndex index;
    private final CollectedFields immediateFields = new CollectedFields();
    private final Map<Set<DeferUsage>, CollectedFields> deferredFields = new LinkedHashMap<>();

    public ExecutionPlan(CollectedFields fields, Set<DeferUsage> parentUsages) {
        index = new DeferUsageIndex(fields);
        fields.getFields().forEach((key, details) -> partition(details, parentUsages));
    }

    private void partition(List<FieldDetails> details, Set<DeferUsage> parentUsages) {
        Set<DeferUsage> usages = filteredUsages(details);
        CollectedFields partition = usages.equals(parentUsages) ? immediateFields : deferredFields.computeIfAbsent(usages, key -> new CollectedFields());
        details.forEach(partition::addField);
    }

    private Set<DeferUsage> filteredUsages(List<FieldDetails> details) {
        Set<DeferUsage> usages = new LinkedHashSet<>();
        for (FieldDetails field : details) {
            if (field.getDeferUsage() == null) return new LinkedHashSet<>();
            usages.add(field.getDeferUsage());
        }
        index.removeDominated(usages);
        return usages;
    }

    public CollectedFields getImmediateFields() {
        return immediateFields;
    }
    public Map<Set<DeferUsage>, CollectedFields> getDeferredFields() {
        return deferredFields;
    }
}
