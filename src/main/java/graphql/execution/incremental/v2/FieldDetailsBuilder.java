package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.language.Field;
import org.jspecify.annotations.NullUnmarked;


@Internal
@NullUnmarked
public final class FieldDetailsBuilder {
    private Field field = null;
    private DeferUsage deferUsage = null;

    public FieldDetailsBuilder field(Field field) {
        this.field = field;
        return this;
    }

    public FieldDetailsBuilder deferUsage(DeferUsage deferUsage) {
        this.deferUsage = deferUsage;
        return this;
    }

    public FieldDetails build() {
        return new FieldDetails(field, deferUsage);
    }
}
