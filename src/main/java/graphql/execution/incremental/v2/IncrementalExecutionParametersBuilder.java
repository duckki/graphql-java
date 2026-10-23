package graphql.execution.incremental.v2;

import graphql.Internal;
import org.jspecify.annotations.NullUnmarked;

import java.util.Collections;
import java.util.Set;
import java.util.List;

@Internal
@NullUnmarked
public final class IncrementalExecutionParametersBuilder {
    private IncrementalWork work = null;
    private CollectedFields collectedFields = null;
    private Set<DeferUsage> parentUsages = Collections.emptySet();
    private DeferContext deferGroups = null;
    private List<DeliveryGroup> declarations = Collections.emptyList();

    public IncrementalExecutionParametersBuilder work(IncrementalWork work) {
        this.work = work;
        return this;
    }

    public IncrementalExecutionParametersBuilder collectedFields(CollectedFields collectedFields) {
        this.collectedFields = collectedFields;
        return this;
    }

    public IncrementalExecutionParametersBuilder parentUsages(Set<DeferUsage> parentUsages) {
        this.parentUsages = parentUsages;
        return this;
    }

    public IncrementalExecutionParametersBuilder deferGroups(DeferContext deferGroups) {
        this.deferGroups = deferGroups;
        return this;
    }

    public IncrementalExecutionParametersBuilder declarations(List<DeliveryGroup> declarations) {
        this.declarations = declarations;
        return this;
    }

    public IncrementalExecutionParameters build() {
        return new IncrementalExecutionParameters(work, collectedFields, parentUsages, deferGroups, declarations);
    }
}
