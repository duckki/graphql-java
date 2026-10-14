package graphql.execution.incremental.v2;

import graphql.Internal;
import graphql.language.Field;
import org.jspecify.annotations.Nullable;

@Internal
public final class FieldDetails {
    private final Field field;
    private final @Nullable DeferUsage deferUsage;

    public FieldDetails(Field field, @Nullable DeferUsage deferUsage) {
        this.field = graphql.Assert.assertNotNull(field);
        this.deferUsage = deferUsage;
    }

    public Field getField() {
        return field;
    }
    public @Nullable DeferUsage getDeferUsage() {
        return deferUsage;
    }

    public static FieldDetailsBuilder newFieldDetails() {
        return new FieldDetailsBuilder();
    }

    public FieldDetails transform(java.util.function.Consumer<FieldDetailsBuilder> consumer) {
        FieldDetailsBuilder builder = newFieldDetails().field(field).deferUsage(deferUsage);
        consumer.accept(builder);
        return builder.build();
    }
}
