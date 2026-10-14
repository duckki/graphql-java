package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

@Internal
public final class FuzzObservation {
    private final ObjectMapper json = new ObjectMapper();
    private final Map<String, JsonNode> paths = new LinkedHashMap<>();
    private final Set<String> open = new LinkedHashSet<>();
    private JsonNode data;
    private int errors;

    public ObjectNode reconstruct(JsonNode initial, ArrayNode updates) {
        start(initial);
        for (int index = 0; index < updates.size(); index++) applyUpdate(updates.get(index), index == updates.size() - 1);
        require(open.isEmpty(), "IDs remain open after termination");
        require(initial.path("pending").isEmpty() == updates.isEmpty(), "Initial incremental flag and update stream disagree");
        return summary();
    }

    public ObjectNode reconstructPrefix(JsonNode initial, ArrayNode updates) {
        start(initial);
        for (int index = 0; index < updates.size(); index++) {
            boolean terminal = !updates.get(index).path("hasNext").asBoolean();
            require(!terminal || index == updates.size() - 1, "Payload after terminal update");
            applyUpdate(updates.get(index), terminal);
        }
        return summary();
    }

    private void start(JsonNode initial) {
        paths.clear();
        open.clear();
        require(initial.path("pending").isEmpty() ? !initial.has("hasNext") : initial.path("hasNext").asBoolean(), "Invalid initial hasNext");
        data = initial.path("data").deepCopy();
        errors = errorCount(initial.path("errors"));
        announce(initial.path("pending"));
    }

    private ObjectNode summary() {
        ObjectNode result = json.createObjectNode().put("errors", errors);
        result.set("data", data); return result;
    }

    private void applyUpdate(JsonNode update, boolean last) {
        require(update.has("hasNext") && update.path("hasNext").isBoolean() && update.path("hasNext").asBoolean() != last, "hasNext must be false only on the final update");
        announce(update.path("pending"));
        update.path("incremental").forEach(this::applyPatch);
        update.path("completed").forEach(this::complete);
    }

    private void announce(JsonNode notices) {
        for (JsonNode notice : notices) {
            require(notice.path("id").isTextual() && notice.path("path").isArray(), "Pending notice must contain a string ID and path array");
            String id = notice.path("id").asText();
            require(!paths.containsKey(id), "Duplicate pending ID " + id);
            paths.put(id, notice.get("path")); open.add(id);
        }
    }

    private void applyPatch(JsonNode patch) {
        String id = patch.path("id").asText();
        require(open.contains(id), "Patch refers to closed or unknown ID " + id);
        JsonNode target = at(data, paths.get(id));
        if (patch.has("subPath")) target = at(target, patch.get("subPath"));
        mergeFields(target, patch.get("data"));
        errors += errorCount(patch.path("errors"));
    }

    private JsonNode at(JsonNode root, JsonNode path) {
        JsonNode current = root;
        for (JsonNode segment : path) {
            require(current != null && current.isContainerNode(), "Patch path traverses a missing parent");
            current = segment.isIntegralNumber() ? current.get(segment.asInt()) : current.get(segment.asText());
        }
        require(current != null, "Patch target is missing");
        return current;
    }

    private void mergeFields(JsonNode target, JsonNode fields) {
        require(target.isObject() && fields != null && fields.isObject(), "Defer patch target is not an object");
        fields.properties().forEach(entry -> ((ObjectNode) target).set(entry.getKey(), entry.getValue().deepCopy()));
    }

    private void complete(JsonNode notice) {
        require(open.remove(notice.path("id").asText()), "Duplicate or unknown completed ID");
        errors += errorCount(notice.path("errors"));
    }

    private int errorCount(JsonNode errors) { return errors.isArray() ? errors.size() : errors.asInt(0); }
    private void require(boolean condition, String message) { if (!condition) throw new DifferentialMismatch(message); }
}
