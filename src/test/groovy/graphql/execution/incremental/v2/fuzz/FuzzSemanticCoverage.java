package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

@Internal
public final class FuzzSemanticCoverage {
    public static final String[] REQUIRED = {"wire.ordinary", "wire.initial-pending", "wire.multiple-pending", "wire.child-pending",
            "wire.defer-patch", "wire.stream-patch", "wire.patch-errors", "wire.completion-failure", "wire.null-items",
            "syntax.label.absent", "syntax.label.null", "syntax.label.string", "syntax.depth.6+", "syntax.nested-list",
            "syntax.interface", "syntax.union", "wire.item-type-patch", "wire.other-type-patch",
            "wire.initial-and-completion-errors", "wire.empty-stream-completion"};
    private final ObjectMapper json = new ObjectMapper();
    public final ObjectNode bins = json.createObjectNode();
    public final ObjectNode witnesses = json.createObjectNode();
    public final ObjectNode stats = json.createObjectNode();
    private final Set<String> inputHashes = new LinkedHashSet<>();
    private final Set<String> queries = new LinkedHashSet<>();
    private final Path output;
    private final boolean retain;

    public FuzzSemanticCoverage(Path output, boolean retain) { this.output = output; this.retain = retain; }

    public void input(FuzzInput input) throws Exception {
        ObjectNode value = inputJson(input);
        value.remove(Arrays.asList("seed", "family"));
        inputHashes.add(hash(value.toString()));
        queries.add(input.query);
        stats.put("uniqueInputs", inputHashes.size()).put("uniqueOperations", queries.size());
    }

    public void record(FuzzInput input, FuzzComparison comparison, String mode, String document, String demand) throws Exception {
        Set<String> reached = new LinkedHashSet<>();
        syntax(input.selections, 0, reached);
        reached.add("family." + input.family);
        reached.add("resolver." + mode);
        reached.add("document." + document);
        reached.add("demand." + demand);
        JsonNode initial = comparison.actual.path("initial");
        reached.add(initial.path("pending").isEmpty() ? "wire.ordinary" : "wire.initial-pending");
        if (initial.path("pending").size() > 1) reached.add("wire.multiple-pending");
        if (!initial.path("errors").isEmpty()) reached.add("wire.initial-errors");
        deliveryBoundaries(comparison.actual, reached);
        for (JsonNode update : comparison.actual.path("updates")) update(update, reached);
        String witness = retain ? retain(input, comparison, mode, document, demand, reached) : null;
        for (String bin : reached) recordBin(bin, input, mode, document, demand, witness);
    }

    private void deliveryBoundaries(JsonNode result, Set<String> reached) {
        JsonNode initial = result.path("initial");
        Set<String> emptyStreams = new LinkedHashSet<>();
        for (JsonNode pending : initial.path("pending")) {
            if (atPath(initial.path("data"), pending.path("path")).isArray()) emptyStreams.add(pending.path("id").asText());
        }
        for (JsonNode update : result.path("updates")) {
            for (JsonNode patch : update.path("incremental")) emptyStreams.remove(patch.path("id").asText());
        }
        for (JsonNode update : result.path("updates")) completionBoundaries(update, initial, emptyStreams, reached);
    }

    private void completionBoundaries(JsonNode update, JsonNode initial, Set<String> emptyStreams, Set<String> reached) {
        for (JsonNode completed : update.path("completed")) {
            if (!completed.path("errors").isEmpty() && !initial.path("errors").isEmpty()) reached.add("wire.initial-and-completion-errors");
            if (completed.path("errors").isEmpty() && emptyStreams.contains(completed.path("id").asText())) reached.add("wire.empty-stream-completion");
        }
    }

    private JsonNode atPath(JsonNode value, JsonNode path) {
        JsonNode current = value;
        for (JsonNode segment : path) current = segment.isIntegralNumber() ? current.path(segment.asInt()) : current.path(segment.asText());
        return current;
    }

    private void syntax(JsonNode nodes, int depth, Set<String> reached) {
        if (nodes.isEmpty()) return;
        max("selectionDepth", depth);
        if (depth >= 6) reached.add("syntax.depth.6+");
        for (JsonNode node : nodes) syntaxNode(node, depth, reached);
    }

    private void syntaxNode(JsonNode node, int depth, Set<String> reached) {
        String name = node.path("name").asText();
        if (name.equals("matrix") || name.equals("strictMatrix")) reached.add("syntax.nested-list");
        if (name.equals("search")) reached.add("syntax.union");
        if (node.path("typeCondition").asText().equals("Named") || node.path("typeCondition").asText().equals("Node")) reached.add("syntax.interface");
        for (JsonNode directive : node.path("directives")) directive(directive, reached);
        syntax(node.path("selections"), depth + 1, reached);
    }

    private void directive(JsonNode directive, Set<String> reached) {
        String name = directive.path("name").asText();
        reached.add("syntax." + name);
        if (name.equals("skip") || name.equals("include")) return;
        if (!directive.has("label")) { reached.add("syntax.label.absent"); return; }
        reached.add(directive.path("label").isNull() ? "syntax.label.null" : "syntax.label.string");
    }

    private void update(JsonNode update, Set<String> reached) {
        if (!update.path("pending").isEmpty()) reached.add("wire.child-pending");
        for (JsonNode patch : update.path("incremental")) patch(patch, reached);
        for (JsonNode completed : update.path("completed")) reached.add(completed.path("errors").isEmpty() ? "wire.completion-success" : "wire.completion-failure");
    }

    private void patch(JsonNode patch, Set<String> reached) {
        reached.add(patch.has("items") ? "wire.stream-patch" : "wire.defer-patch");
        if (!patch.path("errors").isEmpty()) reached.add("wire.patch-errors");
        if (!patch.path("subPath").isEmpty()) reached.add("wire.subpath");
        if (containsField(patch, "itemOnly")) reached.add("wire.item-type-patch");
        if (containsField(patch, "otherOnly")) reached.add("wire.other-type-patch");
        for (JsonNode item : patch.path("items")) if (item.isNull()) reached.add("wire.null-items");
    }

    private boolean containsField(JsonNode value, String name) {
        if (value.has(name)) return true;
        for (JsonNode child : value) if (containsField(child, name)) return true;
        return false;
    }

    private String retain(FuzzInput input, FuzzComparison comparison, String mode, String document, String demand, Set<String> reached) throws Exception {
        if (reached.stream().allMatch(witnesses::has)) return null;
        ObjectNode artifact = json.createObjectNode().put("mode", mode).put("document", document).put("demand", demand);
        artifact.set("input", inputJson(input));
        artifact.set("actual", comparison.actual);
        artifact.set("expected", comparison.expected);
        Path path = output.resolve("witnesses").resolve(hash(input.query + input.root + input.variables) + "-" + mode + "-" + document + "-" + demand + ".json");
        Files.createDirectories(path.getParent());
        json.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), artifact);
        return path.toString();
    }

    private void recordBin(String bin, FuzzInput input, String mode, String document, String demand, String witness) {
        bins.put(bin, bins.path(bin).asInt() + 1);
        if (witnesses.has(bin)) return;
        ObjectNode descriptor = json.createObjectNode().put("seed", input.seed).put("family", input.family)
                .put("resolver", mode).put("document", document).put("demand", demand);
        if (witness != null) descriptor.put("artifact", witness);
        witnesses.set(bin, descriptor);
    }

    public void host(String bin) { bins.put(bin, bins.path(bin).asInt() + 1); }

    public void recordHost(FuzzInput input, ObjectNode observation) throws Exception {
        Set<String> reached = new LinkedHashSet<>();
        if (!observation.path("initial").path("pending").isEmpty()) reached.add("host." + observation.path("policy").asInt());
        if (reached.contains("host.5")) reached.add("host.second-subscriber");
        for (JsonNode source : observation.path("sources")) if (source.path("acquired").asInt() > 0) reached.add("host.closed-source");
        if (reached.stream().allMatch(witnesses::has)) return;
        Path path = output.resolve("witnesses").resolve(input.seed + "-host.json");
        if (retain) saveHost(input, observation, path);
        for (String bin : reached) {
            if (!witnesses.has(bin)) witnesses.set(bin, json.createObjectNode().put("seed", input.seed).put("family", input.family)
                    .put("lane", "host").put("artifact", retain ? path.toString() : ""));
        }
    }

    private void saveHost(FuzzInput input, ObjectNode observation, Path path) throws Exception {
        ObjectNode artifact = json.createObjectNode().put("lane", "host");
        artifact.set("input", inputJson(input));
        artifact.set("actual", observation);
        Files.createDirectories(path.getParent());
        json.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), artifact);
    }

    public void recordFault(FuzzInput input, ObjectNode observation, String mode, String document, String demand) throws Exception {
        String bin = "native.faults." + mode + "." + document;
        host(bin);
        if (witnesses.has(bin)) return;
        Path path = output.resolve("witnesses").resolve(input.seed + "-faults-" + mode + "-" + document + ".json");
        if (retain) saveFault(input, observation, mode, document, demand, path);
        witnesses.set(bin, json.createObjectNode().put("lane", "faults").put("seed", input.seed).put("family", input.family)
                .put("artifact", retain ? path.toString() : ""));
    }

    private void saveFault(FuzzInput input, ObjectNode observation, String mode, String document, String demand, Path path) throws Exception {
        ObjectNode artifact = json.createObjectNode().put("lane", "faults").put("mode", mode).put("document", document).put("demand", demand);
        artifact.set("input", inputJson(input));
        artifact.set("actual", observation);
        Files.createDirectories(path.getParent());
        json.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), artifact);
    }

    public void requireHostBins() {
        for (String bin : Arrays.asList("host.0", "host.1", "host.2", "host.3", "host.4", "host.5", "host.closed-source", "host.second-subscriber")) {
            if (bins.path(bin).asInt() == 0) throw new IllegalStateException("Missing required host coverage bin: " + bin);
        }
    }

    public void requireBins() {
        Set<String> missing = new LinkedHashSet<>(Arrays.asList(REQUIRED));
        missing.removeIf(name -> bins.path(name).asInt() > 0);
        if (!missing.isEmpty()) throw new IllegalStateException("Missing required semantic coverage bins: " + missing);
    }

    private void max(String name, int value) { stats.put(name, Math.max(stats.path(name).asInt(), value)); }

    public ObjectNode inputJson(FuzzInput input) {
        ObjectNode result = json.createObjectNode().put("seed", input.seed).put("family", input.family);
        result.set("selections", input.selections);
        result.set("variables", input.variables);
        result.set("root", input.root);
        return result;
    }

    private String hash(String value) throws Exception {
        byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        StringBuilder result = new StringBuilder();
        for (byte element : bytes) result.append(String.format("%02x", element));
        return result.substring(0, 20);
    }
}
