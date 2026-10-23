package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.Internal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Internal
public final class LeanOracle implements AutoCloseable {
    public static final String MODEL_COMMIT = "aa3f4ee870c7b865b81720aa672d9052a27b3695";
    private final ObjectMapper json = new ObjectMapper();
    private final Process process;
    private final BufferedReader output;
    private final BufferedWriter input;
    private final ExecutorService reader = Executors.newSingleThreadExecutor();
    public final String sourceHash;
    private long id;
    private ObjectNode lastRequest;
    private JsonNode lastResult;

    public LeanOracle(Path project, Path source, Path executable) throws Exception {
        String commit = command(project, "git", "rev-parse", "HEAD");
        if (!MODEL_COMMIT.equals(commit)) throw new IllegalArgumentException("Lean checkout must be pinned to " + MODEL_COMMIT + ", got " + commit);
        if (!command(project, "git", "status", "--porcelain", "--untracked-files=no").isEmpty()) throw new IllegalArgumentException("Lean checkout must have no tracked changes");
        sourceHash = hex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(source)));
        ProcessBuilder builder = executable == null ? new ProcessBuilder("lake", "env", "lean", "--run", source.toString()) : new ProcessBuilder(executable.toString());
        process = builder.directory(project.toFile()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        output = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        input = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
    }

    public JsonNode query(FuzzInput caseInput, JsonNode schema) throws Exception {
        ObjectNode request = json.createObjectNode().put("version", 3).put("executionPolicy", "eager").put("autoSchedule", true);
        request.set("schema", schema); request.set("selections", caseInput.selections); request.set("root", caseInput.root); request.set("variables", caseInput.variables);
        request.set("schedule", json.createArrayNode());
        if (request.equals(lastRequest)) return lastResult;
        ObjectNode cacheKey = request.deepCopy();
        request.put("id", ++id);
        input.write(json.writeValueAsString(request)); input.newLine(); input.flush();
        String line = reader.submit(output::readLine).get(30, TimeUnit.SECONDS);
        if (line == null) throw new IllegalStateException("Lean oracle exited before responding");
        JsonNode response = json.readTree(line);
        if (response.has("error")) throw new IllegalStateException("Lean oracle: " + response.get("error"));
        if (response.path("id").asLong() != id) throw new IllegalStateException("Lean response ID mismatch");
        lastRequest = cacheKey;
        lastResult = response.get("result");
        return lastResult;
    }

    private String command(Path directory, String... arguments) throws Exception {
        Process child = new ProcessBuilder(arguments).directory(directory.toFile()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        String result = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (!child.waitFor(30, TimeUnit.SECONDS) || child.exitValue() != 0) throw new IllegalStateException("Model pin command failed");
        return result;
    }

    private String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte value : bytes) result.append(String.format("%02x", value));
        return result.toString();
    }

    @Override
    public void close() throws Exception {
        try {
            input.write("quit\n"); input.flush();
            if (!process.waitFor(3, TimeUnit.SECONDS)) process.destroyForcibly();
        } finally {
            process.destroy(); reader.shutdownNow(); input.close(); output.close();
        }
    }
}
