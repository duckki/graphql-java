package graphql.execution.incremental.v2.fuzz;

import graphql.Internal;

import java.util.Arrays;
import java.util.List;

@Internal
public final class FuzzProfile {
    public final String name = System.getProperty("fuzzProfile", "ci");
    public final int cases;
    public final int depth;
    public final int selections;
    public final int listLength;
    public final int hostCases;
    public final long seed = Long.parseUnsignedLong(System.getProperty("fuzzSeed", "1"));
    public final long durationSeconds = Long.parseLong(System.getProperty("fuzzDurationSeconds", "0"));
    public final List<String> resolvers;
    public final List<String> documents;
    public final List<String> demands;
    public final boolean requireBins;
    public final boolean retainCorpus;
    public final boolean sentinel;

    public FuzzProfile() {
        if (!Arrays.asList("defer", "legacy", "smoke", "ci", "nightly").contains(name)) throw new IllegalArgumentException("Unknown fuzzProfile " + name);
        cases = Integer.parseInt(System.getProperty("fuzzCases", Integer.toString(defaultCases())));
        boolean small = name.equals("legacy") || name.equals("defer");
        depth = small ? 3 : name.equals("nightly") ? 6 : 4;
        selections = small ? 36 : name.equals("nightly") ? 96 : 64;
        listLength = small ? 3 : name.equals("nightly") ? 8 : 6;
        hostCases = Integer.parseInt(System.getProperty("fuzzHostCases", small ? "0" : name.equals("nightly") ? "1000" : "100"));
        resolvers = choices("fuzzResolvers", small ? "sync,promise" : "sync,promise,items,async", "sync", "promise", "items", "async");
        documents = choices("fuzzDocuments", name.equals("legacy") ? "inline" : "inline,named", "inline", "named");
        demands = choices("fuzzDemands", name.equals("legacy") ? "one" : "one,burst,all", "one", "burst", "all", "unbounded");
        requireBins = Boolean.parseBoolean(System.getProperty("fuzzRequireBins", Boolean.toString(!small && cases > 0 && System.getProperty("fuzzReplay") == null)));
        retainCorpus = Boolean.parseBoolean(System.getProperty("fuzzRetainCorpus", Boolean.toString(!small)));
        sentinel = Boolean.parseBoolean(System.getProperty("fuzzSentinel", "true"));
        if (cases < 0 || hostCases < 0 || durationSeconds < 0 || seed < 0 || seed > 0xffffffffL) throw new IllegalArgumentException("Invalid fuzz case, duration or unsigned 32-bit seed limit");
    }

    private int defaultCases() {
        if (name.equals("smoke")) return 100;
        if (name.equals("nightly")) return 50000;
        return name.equals("legacy") || name.equals("defer") ? 1000 : 2000;
    }

    private List<String> choices(String property, String fallback, String... allowed) {
        List<String> result = Arrays.asList(System.getProperty(property, fallback).split(",", -1));
        if (result.isEmpty() || !Arrays.asList(allowed).containsAll(result)) throw new IllegalArgumentException("Invalid " + property + ": " + result);
        return result;
    }
}
