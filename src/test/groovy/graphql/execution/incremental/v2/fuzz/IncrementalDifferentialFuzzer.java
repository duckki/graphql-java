package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Internal
public final class IncrementalDifferentialFuzzer {
    private final ObjectMapper json = new ObjectMapper();
    private final Path output = Paths.get(System.getProperty("fuzzOutput", "build/incremental-fuzz"));
    private final ObjectNode report = json.createObjectNode();
    private final Map<String, FuzzSchema> schemas = new LinkedHashMap<>();
    private final long seed = Long.getLong("fuzzSeed", 1);
    private final int cases = Integer.getInteger("fuzzCases", 1000);
    private final long started = System.nanoTime();
    private JsonNode replayOptions;
    private int comparisons;
    private int inputs;

    public static void main(String[] arguments) throws Exception {
        new IncrementalDifferentialFuzzer().run();
    }

    private void run() throws Exception {
        String leanProject = System.getProperty("leanProject", System.getenv("GRAPHQL_INCREMENTAL_LEAN_PROJECT"));
        if (leanProject == null) throw new IllegalArgumentException("Set -PleanProject=/path/to/graphql-lean or GRAPHQL_INCREMENTAL_LEAN_PROJECT");
        Path source = Paths.get("fuzz/incremental/lean/NativeOracle.lean").toAbsolutePath();
        String executable = System.getProperty("leanOracle");
        report.put("format", 2).put("modelCommit", LeanOracle.MODEL_COMMIT).put("executionPolicy", "eager")
                .put("scope", "defer").put("seed", seed).put("cases", cases).put("status", "failed")
                .put("javaSourceSha256", sourceHash(Paths.get("src/main/java")))
                .put("harnessSha256", sourceHash(Paths.get("src/test/groovy/graphql/execution/incremental/v2/fuzz")));
        try (LeanOracle oracle = new LeanOracle(Paths.get(leanProject), source, executable == null ? null : Paths.get(executable))) {
            report.put("oracleSourceSha256", oracle.sourceHash);
            runCorpus(oracle);
            if (System.getProperty("fuzzReplay") == null) runGenerated(oracle);
            report.put("status", "passed");
        } finally {
            schemas.values().forEach(FuzzSchema::close);
            report.put("inputs", inputs).put("exactComparisons", comparisons).put("elapsedSeconds", (System.nanoTime() - started) / 1_000_000_000.0);
            Files.createDirectories(output);
            json.writerWithDefaultPrettyPrinter().writeValue(output.resolve("report.json").toFile(), report);
        }
        System.out.println(inputs + " inputs; " + comparisons + " exact comparisons; report: " + output.resolve("report.json"));
    }

    private void runCorpus(LeanOracle oracle) throws Exception {
        String replay = System.getProperty("fuzzReplay");
        if (replay != null) { runArtifact(oracle, Paths.get(replay)); return; }
        Path corpus = Paths.get(System.getProperty("fuzzCorpus", "fuzz/incremental/corpus"));
        try (Stream<Path> files = Files.list(corpus)) {
            for (Path file : files.filter(path -> path.toString().endsWith(".json")).sorted().collect(Collectors.toList())) runArtifact(oracle, file);
        }
    }

    private void runArtifact(LeanOracle oracle, Path path) throws Exception {
        replayOptions = json.readTree(path.toFile());
        try { runInput(oracle, FuzzInput.fromJson(replayOptions.has("input") ? replayOptions.get("input") : replayOptions)); }
        finally { replayOptions = null; }
    }

    private void runGenerated(LeanOracle oracle) throws Exception {
        DeferInputGenerator generator = new DeferInputGenerator(schema("sync").getSchema());
        for (int index = 0; index < cases; index++) runInput(oracle, generator.generate((seed + index) & 0xffffffffL));
    }

    private FuzzSchema schema(String mode) {
        return schemas.computeIfAbsent(mode, FuzzSchema::new);
    }

    private void runInput(LeanOracle oracle, FuzzInput input) throws Exception {
        inputs++;
        int variant = 0;
        for (String mode : selected("fuzzResolvers", "mode", "sync,promise")) {
            for (String document : selected("fuzzDocuments", "document", "inline,named")) {
                List<String> demands = selected("fuzzDemands", "demand", "one,burst,unbounded");
                compare(oracle, input, mode, document, demands.get((int) Math.floorMod(input.seed + variant++, demands.size())));
            }
        }
    }

    private List<String> selected(String property, String key, String fallback) {
        if (System.getProperty(property) == null && replayOptions != null && replayOptions.has(key)) return List.of(replayOptions.path(key).asText());
        return List.of(System.getProperty(property, fallback).split(","));
    }

    private void compare(LeanOracle oracle, FuzzInput input, String mode, String document, String demand) throws Exception {
        FuzzComparison comparison = new FuzzComparison(oracle, schema(mode), document, demand);
        try {
            comparison.check(input);
            comparisons++;
            sentinel(comparison);
        } catch (Exception mismatch) {
            saveFailure(input, mode, document, demand, comparison, mismatch);
            throw mismatch;
        }
    }

    private void sentinel(FuzzComparison comparison) throws Exception {
        if (!Boolean.parseBoolean(System.getProperty("fuzzSentinel", "true")) || report.path("sentinelDetected").asBoolean()) return;
        ObjectNode corrupted = comparison.expectedSummary.deepCopy();
        corrupted.put("errors", corrupted.path("errors").asInt() + 1);
        detectSummary(comparison, corrupted);
        corrupted = comparison.expectedSummary.deepCopy();
        corrupted.set("data", json.createObjectNode().put("sentinel", true));
        detectSummary(comparison, corrupted);
        JsonNode initial = json.readTree("{\"data\":{},\"pending\":[{\"id\":\"0\",\"path\":[]}],\"hasNext\":true}");
        ArrayNode updates = (ArrayNode) json.readTree("[{\"completed\":[{\"id\":\"unknown\"}],\"hasNext\":false}]");
        try { new FuzzObservation().reconstruct(initial, updates); throw new IllegalStateException("Observer accepted an unknown completion ID"); }
        catch (DifferentialMismatch detected) { report.put("sentinelDetected", true); }
    }

    private void detectSummary(FuzzComparison comparison, ObjectNode corrupted) {
        try { comparison.assertEqual(comparison.actualSummary, corrupted); }
        catch (DifferentialMismatch detected) { return; }
        throw new IllegalStateException("Comparator accepted deliberately corrupted data");
    }

    private void saveFailure(FuzzInput original, String mode, String document, String demand, FuzzComparison comparison, Exception mismatch) throws Exception {
        JsonNode originalActual = comparison.actual;
        JsonNode originalExpected = comparison.expected;
        FuzzInput minimized = mismatch instanceof DifferentialMismatch ? new FuzzShrinker(comparison).minimize(original) : original;
        try { comparison.check(minimized); } catch (Exception reproduced) { mismatch = reproduced; }
        ObjectNode artifact = json.createObjectNode().put("format", 2).put("modelCommit", LeanOracle.MODEL_COMMIT)
                .put("mode", mode).put("document", document).put("demand", demand).put("error", mismatch.toString());
        artifact.set("input", inputJson(minimized));
        artifact.set("originalInput", inputJson(original));
        artifact.set("actual", comparison.actual);
        artifact.set("expected", comparison.expected);
        artifact.set("originalActual", originalActual);
        artifact.set("originalExpected", originalExpected);
        Path path = output.resolve("failures").resolve(original.seed + "-" + mode + "-" + document + ".json");
        Files.createDirectories(path.getParent());
        json.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), artifact);
        report.put("failureArtifact", path.toString());
        System.err.println("Failure saved to " + path + "; replay with -PfuzzReplay=" + path);
    }

    private ObjectNode inputJson(FuzzInput input) {
        ObjectNode result = json.createObjectNode().put("seed", input.seed).put("family", input.family);
        result.set("selections", input.selections);
        result.set("variables", input.variables);
        result.set("root", input.root);
        return result;
    }

    private String sourceHash(Path directory) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path file : files.filter(Files::isRegularFile).sorted().collect(Collectors.toList())) {
                digest.update(directory.relativize(file).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                digest.update(Files.readAllBytes(file));
            }
        }
        StringBuilder result = new StringBuilder();
        for (byte value : digest.digest()) result.append(String.format("%02x", value));
        return result.toString();
    }
}
