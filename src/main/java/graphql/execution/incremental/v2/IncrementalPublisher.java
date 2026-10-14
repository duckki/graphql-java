package graphql.execution.incremental.v2;

import graphql.GraphQLError;
import graphql.Internal;
import graphql.incremental.CompletedPayload;
import graphql.incremental.IncrementalPayloadV2;
import graphql.incremental.PendingPayload;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static graphql.Assert.assertNotNull;

@Internal
public final class IncrementalPublisher {
    private final Map<Object, String> ids = new IdentityHashMap<>();
    private long nextId;
    private boolean finished;

    public PendingPayload pending(DeliveryGroup group) {
        return PendingPayload.newPendingPayload().id(ensureId(group)).path(group.getPath().toList()).label(group.getLabel()).labelPresent(group.hasLabel()).build();
    }

    public PendingPayload pending(ItemStream stream) {
        return PendingPayload.newPendingPayload().id(ensureId(stream)).path(stream.getPath().toList()).label(stream.getLabel()).labelPresent(stream.hasLabel()).build();
    }

    @SuppressWarnings("unchecked")
    public IncrementalPayloadV2 deferred(DeliveryGroup seed, DeferredTask task, WorkResult result) {
        DeliveryGroup best = seed;
        for (DeliveryGroup candidate : task.getGroups()) {
            if (ids.containsKey(candidate) && candidate.getPath().toList().size() > best.getPath().toList().size()) best = candidate;
        }
        List<Object> path = task.getPath().toList();
        return IncrementalPayloadV2.newIncrementalPayloadV2().id(assertNotNull(ids.get(best)))
                .subPath(path.subList(best.getPath().toList().size(), path.size()))
                .data((Map<String, Object>) result.getValue()).errors(result.getErrors()).build();
    }

    public IncrementalPayloadV2 streamed(ItemStream stream, WorkResult result) {
        return IncrementalPayloadV2.newIncrementalPayloadV2().id(assertNotNull(ids.get(stream)))
                .items(Collections.singletonList(result.getValue())).errors(result.getErrors()).build();
    }

    public CompletedPayload completed(Object record, List<GraphQLError> errors) {
        String id = assertNotNull(ids.remove(record));
        return CompletedPayload.newCompletedPayload().id(id).errors(errors).build();
    }

    public void finish() {
        finished = true;
        ids.clear();
    }

    private String ensureId(Object record) {
        graphql.Assert.assertTrue(!finished, "The incremental publisher is closed");
        return ids.computeIfAbsent(record, key -> Long.toString(nextId++));
    }
}
