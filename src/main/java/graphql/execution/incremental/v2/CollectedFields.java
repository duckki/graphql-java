package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.execution.MergedField;
import graphql.execution.MergedSelectionSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Internal
public final class CollectedFields {
    private final Map<String, List<FieldDetails>> fields = new LinkedHashMap<>();
    private final List<DeferUsage> newDeferUsages = new ArrayList<>();

    public void addField(FieldDetails details) {
        fields.computeIfAbsent(details.getField().getResultKey(), key -> new ArrayList<>()).add(details);
    }

    public void addDeferUsage(DeferUsage usage) {
        newDeferUsages.add(usage);
    }
    public Map<String, List<FieldDetails>> getFields() {
        return fields;
    }
    public List<DeferUsage> getNewDeferUsages() {
        return newDeferUsages;
    }

    public MergedSelectionSet toMergedSelectionSet() {
        Map<String, MergedField> merged = new LinkedHashMap<>();
        fields.forEach((key, details) -> merged.put(key, merge(details)));
        return MergedSelectionSet.newMergedSelectionSet().subFields(merged).build();
    }

    private MergedField merge(List<FieldDetails> details) {
        MergedField.Builder builder = MergedField.newMergedField();
        details.forEach(detail -> builder.addField(detail.getField()));
        return builder.build();
    }
}
