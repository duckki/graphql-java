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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Internal
public final class IncrementalDifferentialFuzzer {
    private final ObjectMapper json = new ObjectMapper();
    private final FuzzProfile profile = new FuzzProfile();
    private final Path output = Paths.get(System.getProperty("fuzzOutput", "build/incremental-fuzz"));
    private final ObjectNode report = json.createObjectNode();
    private final FuzzSemanticCoverage coverage = new FuzzSemanticCoverage(output, profile.retainCorpus);
    private final ArrayNode failures = json.createArrayNode();
    private final Map<String, FuzzSchema> schemas = new LinkedHashMap<>();
    private final long started = System.nanoTime();
    private JsonNode replayOptions;
    private int comparisons;
    private int inputs;
    private int generated;
    private int hostChecks;
    private int exceptionalItemChecks;
    private int validationChecks;
    private int faultChecks;

    public static void main(String[] arguments) throws Exception { new IncrementalDifferentialFuzzer().run(); }

    private void run() throws Exception {
        String leanProject = System.getProperty("leanProject", System.getenv("GRAPHQL_INCREMENTAL_LEAN_PROJECT"));
        if (leanProject == null) throw new IllegalArgumentException("Set -PleanProject=/path/to/graphql-lean or GRAPHQL_INCREMENTAL_LEAN_PROJECT");
        Path source = Paths.get("fuzz/incremental/lean/NativeOracle.lean").toAbsolutePath();
        String executable = System.getProperty("leanOracle");
        initializeReport(source);
        try (LeanOracle oracle = new LeanOracle(Paths.get(leanProject), source, executable == null ? null : Paths.get(executable))) {
            report.put("oracleSourceSha256", oracle.sourceHash);
            runCorpus(oracle);
            runExceptionalItems();
            runValidation();
            runFaultCorpus();
            runGenerated(oracle);
            if (profile.requireBins) coverage.requireBins();
            if (profile.requireBins && profile.hostCases >= 60 && generated >= 60) coverage.requireHostBins();
            report.put("status", "passed");
        } finally { schemas.values().forEach(FuzzSchema::close); writeReport(); }
        System.out.println(inputs + " inputs; " + comparisons + " exact comparisons; " + hostChecks + " host checks; report: " + output.resolve("report.json"));
        if (!failures.isEmpty()) throw new IllegalStateException("Differential fuzzing failed; see " + output.resolve("report.json"));
    }

    private void initializeReport(Path source) throws Exception {
        report.put("format", 2).put("modelCommit", LeanOracle.MODEL_COMMIT).put("executionPolicy", "eager")
                .put("generatedErrorDomain", "nullable structural errors and targeted single-origin fatal outcomes")
                .put("javaVersion", System.getProperty("java.version")).put("javaSourceSha256", sourceHash(Paths.get("src/main/java")))
                .put("harnessSha256", sourceHash(Paths.get("src/test/groovy/graphql/execution/incremental/v2/fuzz")))
                .put("seed", profile.seed).put("cases", profile.cases).put("profile", profile.name)
                .put("selectionBudget", profile.selections).put("structuralDepth", profile.depth).put("maximumListLength", profile.listLength)
                .put("durationLimitSeconds", profile.durationSeconds).put("oracleSource", source.toString()).put("status", "failed");
        report.set("resolvers", json.valueToTree(profile.resolvers));
        report.set("documents", json.valueToTree(profile.documents));
        report.set("demands", json.valueToTree(profile.demands));
        report.set("coverage", coverage.bins);
        report.set("witnesses", coverage.witnesses);
        report.set("statistics", coverage.stats);
        report.set("failures", failures);
    }

    private void runCorpus(LeanOracle oracle) throws Exception {
        String replay = System.getProperty("fuzzReplay");
        if (replay != null) {
            replayOptions = json.readTree(Paths.get(replay).toFile());
            runArtifact(oracle, Paths.get(replay));
            return;
        }
        Path corpus = Paths.get(System.getProperty("fuzzCorpus", "fuzz/incremental/corpus"));
        if (!Files.isDirectory(corpus)) throw new IllegalArgumentException("Corpus directory does not exist: " + corpus);
        try (Stream<Path> files = Files.list(corpus)) {
            for (Path file : files.filter(path -> path.toString().endsWith(".json")).sorted().collect(Collectors.toList())) runCorpusArtifact(oracle, file);
        }
    }

    private void runCorpusArtifact(LeanOracle oracle, Path file) throws Exception {
        replayOptions = json.readTree(file.toFile());
        if (profile.name.equals("defer") && System.getProperty("fuzzCorpus") == null
                && readInput(file).query.contains("@stream")) { replayOptions = null; return; }
        try { runArtifact(oracle, file); }
        finally { replayOptions = null; }
    }

    private void runArtifact(LeanOracle oracle, Path file) throws Exception {
        String lane = replayOptions.path("lane").asText();
        if (lane.equals("host")) { runHost(readInput(file)); return; }
        if (lane.equals("faults")) { runFaults(readInput(file)); return; }
        if (lane.equals("validation")) {
            new FuzzValidationChecks(coverage).check(replayOptions.path("seed").asLong());
            validationChecks++;
            return;
        }
        if (lane.equals("item-rejection")) {
            new FuzzHostChecks(schema("async"), coverage).checkRejectedItems(replayOptions.path("strict").asBoolean(), replayOptions.path("prefix").asInt());
            exceptionalItemChecks++;
            return;
        }
        runInput(oracle, readInput(file));
    }

    private FuzzInput readInput(Path path) throws Exception {
        JsonNode artifact = json.readTree(path.toFile());
        return FuzzInput.fromJson(artifact.has("input") ? artifact.get("input") : artifact);
    }

    private void runGenerated(LeanOracle oracle) throws Exception {
        if (System.getProperty("fuzzReplay") != null) return;
        FuzzScenarioGenerator generator = new FuzzScenarioGenerator(profile, schema("sync").getSchema());
        for (int index = 0; index < profile.cases && !durationExpired(); index++) {
            long seed = (profile.seed + index) & 0xffffffffL;
            FuzzInput input = profile.name.equals("defer") ? new DeferInputGenerator(schema("sync").getSchema()).generate(seed)
                    : profile.name.equals("legacy") ? legacyInput(seed) : generator.generate(seed);
            runInput(oracle, input);
            if (generated < profile.hostCases) runHost(input);
            if (generated < profile.hostCases && !profile.name.equals("legacy") && input.family.equals("structural")) runFaults(generator.generateFaults(seed));
            generated++;
            if (generated % 100 == 0) System.out.println(generated + " generated inputs; " + comparisons + " exact comparisons");
        }
    }

    private FuzzInput legacyInput(long seed) {
        FuzzInputGenerator generator = new FuzzInputGenerator();
        FuzzInput input = generator.generate(seed);
        if (generator.getFallbackReads() > 0) coverage.stats.put("entropyExhaustedInputs", coverage.stats.path("entropyExhaustedInputs").asInt() + 1);
        coverage.stats.put("entropyFallbackReads", coverage.stats.path("entropyFallbackReads").asLong() + generator.getFallbackReads());
        return input;
    }

    private boolean durationExpired() {
        return profile.durationSeconds > 0 && (System.nanoTime() - started) / 1_000_000_000L >= profile.durationSeconds;
    }

    private FuzzSchema schema(String mode) { return schemas.computeIfAbsent(mode, FuzzSchema::new); }

    private void runExceptionalItems() throws Exception {
        if (profile.hostCases == 0 || profile.cases == 0 || replayOptions != null) return;
        FuzzHostChecks host = new FuzzHostChecks(schema("async"), coverage);
        for (boolean strict : new boolean[]{false, true}) {
            for (int prefix : new int[]{0, 1}) checkExceptional(host, strict, prefix);
        }
    }

    private void checkExceptional(FuzzHostChecks host, boolean strict, int prefix) throws Exception {
        try { host.checkRejectedItems(strict, prefix); exceptionalItemChecks++; }
        catch (Exception failure) { saveExceptionalFailure(host, strict, prefix, failure); throw failure; }
    }

    private void saveExceptionalFailure(FuzzHostChecks host, boolean strict, int prefix, Exception failure) throws Exception {
        ObjectNode artifact = json.createObjectNode().put("format", 2).put("lane", "item-rejection").put("strict", strict)
                .put("prefix", prefix).put("error", failure.toString());
        artifact.set("input", inputJson(host.rejectedInput));
        artifact.set("actual", host.observation);
        Path path = output.resolve("failures").resolve("rejected-" + strict + "-" + prefix + ".json");
        Files.createDirectories(path.getParent());
        json.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), artifact);
        failures.add(path.toString());
        System.err.println("Item rejection failure saved to " + path + "; replay with -PfuzzReplay=" + path);
    }
    private void runValidation() throws Exception {
        if (profile.hostCases == 0 || profile.cases == 0 || replayOptions != null) return;
        FuzzValidationChecks validator = new FuzzValidationChecks(coverage);
        for (int seed = 0; seed < 128; seed++) {
            try { validator.check(seed); validationChecks++; }
            catch (Exception failure) { saveValidationFailure(seed, validator, failure); throw failure; }
        }
    }

    private void saveValidationFailure(long seed, FuzzValidationChecks validator, Exception failure) throws Exception {
        ObjectNode artifact = json.createObjectNode().put("format", 2).put("lane", "validation").put("seed", seed).put("error", failure.toString());
        artifact.set("actual", validator.observation);
        Path path = output.resolve("failures").resolve(seed + "-validation.json");
        Files.createDirectories(path.getParent());
        json.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), artifact);
        failures.add(path.toString());
        System.err.println("Validation failure saved to " + path + "; replay with -PfuzzReplay=" + path);
    }

    private void runHost(FuzzInput input) throws Exception {
        FuzzHostChecks host = new FuzzHostChecks(schema("async"), coverage);
        try {
            host.check(input);
            coverage.recordHost(input, host.observation);
            hostChecks++;
        } catch (Exception failure) {
            saveHostFailure(input, host, failure);
            throw failure;
        }
    }

    private void saveHostFailure(FuzzInput original, FuzzHostChecks host, Exception failure) throws Exception {
        FuzzInput minimized = failure instanceof DifferentialMismatch ? new FuzzShrinker((FuzzInputCheck) host::check).minimize(original) : original;
        try { host.check(minimized); } catch (Exception reproduced) { failure = reproduced; }
        ObjectNode artifact = json.createObjectNode().put("format", 2).put("lane", "host").put("error", failure.toString())
                .put("modelCommit", LeanOracle.MODEL_COMMIT).put("query", new FuzzQueryRenderer(schema("sync").getSchema()).render(minimized.selections, true));
        artifact.set("input", inputJson(minimized));
        artifact.set("originalInput", inputJson(original));
        artifact.set("actual", host.observation);
        Path path = output.resolve("failures").resolve(minimized.seed + "-host.json");
        Files.createDirectories(path.getParent());
        json.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), artifact);
        failures.add(path.toString());
        System.err.println("Host failure saved to " + path + "; replay with -PfuzzReplay=" + path);
    }

    private void runInput(LeanOracle oracle, FuzzInput input) throws Exception {
        inputs++;
        coverage.input(input);
        int variant = 0;
        for (String mode : selected("fuzzResolvers", "mode", profile.resolvers)) {
            for (String document : selected("fuzzDocuments", "document", profile.documents)) {
                String demand = profile.demands.get((int) Math.floorMod(input.seed + variant++, profile.demands.size()));
                compare(oracle, input, mode, document, replayValue("fuzzDemands", "demand", demand));
            }
        }
    }

    private void runFaults(FuzzInput input) throws Exception {
        for (String mode : selected("fuzzResolvers", "mode", profile.resolvers)) {
            for (String document : selected("fuzzDocuments", "document", profile.documents)) runFault(input, mode, document);
        }
    }

    private void runFaultCorpus() throws Exception {
        if (profile.hostCases == 0 || profile.cases == 0 || replayOptions != null) return;
        try (Stream<Path> files = Files.list(Paths.get("fuzz/incremental/native-corpus"))) {
            for (Path file : files.filter(path -> path.toString().endsWith(".json")).sorted().collect(Collectors.toList())) runFaults(readInput(file));
        }
    }

    private void runFault(FuzzInput input, String mode, String document) throws Exception {
        String demand = replayValue("fuzzDemands", "demand", profile.demands.get((int) (input.seed % profile.demands.size())));
        FuzzFaultChecks checker = new FuzzFaultChecks(schema(mode), document, demand);
        try {
            checker.check(input);
            faultChecks++;
            coverage.recordFault(input, checker.observation, mode, document, demand);
        } catch (Exception failure) {
            saveFaultFailure(input, checker, mode, document, demand, failure);
            throw failure;
        }
    }

    private void saveFaultFailure(FuzzInput input, FuzzFaultChecks checker, String mode, String document, String demand, Exception failure) throws Exception {
        JsonNode original = checker.observation;
        FuzzInput minimized = failure instanceof DifferentialMismatch ? new FuzzShrinker((FuzzInputCheck) checker::check).minimize(input) : input;
        try { checker.check(minimized); } catch (Exception reproduced) { failure = reproduced; }
        ObjectNode artifact = json.createObjectNode().put("format", 2).put("lane", "faults").put("mode", mode).put("document", document)
                .put("demand", demand).put("error", failure.toString()).put("query", new FuzzQueryRenderer(schema("sync").getSchema()).render(minimized.selections, document.equals("named")));
        artifact.set("input", inputJson(minimized));
        artifact.set("originalInput", inputJson(input));
        artifact.set("actual", checker.observation);
        artifact.set("originalActual", original);
        Path path = output.resolve("failures").resolve(input.seed + "-faults-" + mode + "-" + document + ".json");
        Files.createDirectories(path.getParent());
        json.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), artifact);
        failures.add(path.toString());
        System.err.println("Fault property failure saved to " + path + "; replay with -PfuzzReplay=" + path);
    }

    private List<String> selected(String property, String key, List<String> fallback) {
        if (replayOptions == null || System.getProperty(property) != null || !replayOptions.has(key)) return fallback;
        return List.of(replayOptions.path(key).asText());
    }

    private String replayValue(String property, String key, String fallback) {
        return replayOptions != null && System.getProperty(property) == null ? replayOptions.path(key).asText(fallback) : fallback;
    }

    private void compare(LeanOracle oracle, FuzzInput input, String mode, String document, String demand) throws Exception {
        FuzzComparison comparison = new FuzzComparison(oracle, schema(mode), document, demand);
        try {
            comparison.check(input);
            comparisons++;
            coverage.record(input, comparison, mode, document, demand);
            sentinel(comparison);
        } catch (Exception mismatch) {
            saveFailure(input, mode, document, demand, comparison, mismatch);
            throw mismatch;
        }
    }

    private void sentinel(FuzzComparison comparison) throws Exception {
        if (!profile.sentinel || report.path("sentinelDetected").asBoolean()) return;
        ObjectNode corrupted = comparison.expectedSummary.deepCopy();
        corrupted.put("errors", corrupted.path("errors").asInt() + 1);
        detectSummary(comparison, corrupted);
        corrupted = comparison.expectedSummary.deepCopy();
        corrupted.set("data", json.createObjectNode().put("sentinel", true));
        detectSummary(comparison, corrupted);
        detectLifecycle();
        report.put("sentinelDetected", true);
        report.set("sentinelChecks", json.valueToTree(List.of("error-count", "data", "unknown-id")));
    }

    private void detectSummary(FuzzComparison comparison, ObjectNode corrupted) {
        try { comparison.assertEqual(comparison.actualSummary, corrupted); }
        catch (DifferentialMismatch detected) { return; }
        throw new IllegalStateException("Comparator failed to detect deliberately corrupted oracle result");
    }

    private void detectLifecycle() throws Exception {
        JsonNode initial = json.readTree("{\"data\":{},\"pending\":[{\"id\":\"0\",\"path\":[]}],\"hasNext\":true}");
        ArrayNode updates = (ArrayNode) json.readTree("[{\"completed\":[{\"id\":\"unknown\"}],\"hasNext\":false}]");
        try { new FuzzObservation().reconstruct(initial, updates); }
        catch (DifferentialMismatch detected) { return; }
        throw new IllegalStateException("Observer accepted an unknown completion ID");
    }
    private void saveFailure(FuzzInput original, String mode, String document, String demand, FuzzComparison comparison, Exception mismatch) throws Exception {
        JsonNode originalActual = comparison.actual;
        JsonNode originalExpected = comparison.expected;
        String originalError = mismatch.toString();
        FuzzInput input = mismatch instanceof DifferentialMismatch ? new FuzzShrinker(comparison).minimize(original) : original;
        try { comparison.check(input); } catch (Exception reproduced) { mismatch = reproduced; }
        ObjectNode artifact = json.createObjectNode().put("format", 2).put("modelCommit", LeanOracle.MODEL_COMMIT)
                .put("mode", mode).put("document", document).put("demand", demand).put("profile", profile.name)
                .put("error", mismatch.toString()).put("query", new FuzzQueryRenderer(schema("sync").getSchema()).render(input.selections, document.equals("named")));
        artifact.set("input", inputJson(input)); artifact.set("originalInput", inputJson(original));
        artifact.set("actual", comparison.actual); artifact.set("expected", comparison.expected);
        artifact.set("actualSummary", comparison.actualSummary); artifact.set("expectedSummary", comparison.expectedSummary);
        artifact.set("originalActual", originalActual); artifact.set("originalExpected", originalExpected);
        artifact.put("originalError", originalError);
        Path path = output.resolve("failures").resolve(input.seed + "-" + mode + "-" + document + "-" + demand + ".json");
        Files.createDirectories(path.getParent());
        json.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), artifact);
        failures.add(path.toString());
        System.err.println("Mismatch saved to " + path + "; replay with -PfuzzReplay=" + path);
    }

    private ObjectNode inputJson(FuzzInput input) {
        return coverage.inputJson(input);
    }

    private String sourceHash(Path root) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (Stream<Path> stream = Files.walk(root)) {
            List<Path> files = stream.filter(Files::isRegularFile).sorted(Comparator.comparing(Path::toString)).collect(Collectors.toList());
            for (Path file : files) {
                digest.update(root.relativize(file).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                digest.update(Files.readAllBytes(file));
            }
        }
        StringBuilder result = new StringBuilder();
        for (byte value : digest.digest()) result.append(String.format("%02x", value));
        return result.toString();
    }

    private void writeReport() throws Exception {
        double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
        report.put("inputs", inputs).put("generated", generated).put("comparisons", comparisons).put("hostChecks", hostChecks)
                .put("exceptionalItemChecks", exceptionalItemChecks).put("validationChecks", validationChecks)
                .put("faultChecks", faultChecks)
                .put("elapsedSeconds", seconds).put("comparisonsPerSecond", comparisons / seconds);
        Files.createDirectories(output);
        json.writerWithDefaultPrettyPrinter().writeValue(output.resolve("report.json").toFile(), report);
    }
}
