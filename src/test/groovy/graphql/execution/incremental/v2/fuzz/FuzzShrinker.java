package graphql.execution.incremental.v2.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import graphql.Internal;

@Internal
public final class FuzzShrinker {
    private final FuzzInputCheck check;
    private int attempts;
    private String signature;

    public FuzzShrinker(FuzzComparison comparison) { this(comparison::check); }
    public FuzzShrinker(FuzzInputCheck check) { this.check = check; }

    public FuzzInput minimize(FuzzInput original) throws Exception {
        attempts = 0;
        signature = failureSignature(original);
        if (signature == null) return original;
        FuzzInput current = original;
        boolean reduced = true;
        while (reduced && attempts < 100) {
            FuzzInput candidate = removeOne(current, current.selections, new int[0]);
            reduced = candidate != current;
            current = candidate;
        }
        return current;
    }

    private FuzzInput removeOne(FuzzInput input, ArrayNode selections, int[] address) throws Exception {
        for (int index = 0; index < selections.size() && attempts < 100; index++) {
            FuzzInput reduced = tryRemove(input, selections, address, index);
            if (reduced != input) return reduced;
            int[] childAddress = java.util.Arrays.copyOf(address, address.length + 1); childAddress[address.length] = index;
            FuzzInput nested = removeOne(input, (ArrayNode) selections.get(index).get("selections"), childAddress);
            if (nested != input) return nested;
        }
        return input;
    }

    private FuzzInput tryRemove(FuzzInput input, ArrayNode selections, int[] address, int index) throws Exception {
        if (selections.size() <= 1) return input;
        ArrayNode copy = input.selections.deepCopy();
        ArrayNode target = copy;
        for (int position : address) target = (ArrayNode) target.get(position).get("selections");
        target.remove(index);
        FuzzInput candidate = new FuzzInput(input.seed, copy, input.variables, input.root, input.family);
        attempts++;
        try { check.check(candidate); return input; }
        catch (IllegalArgumentException invalid) { return input; }
        catch (DifferentialMismatch mismatch) { return signature.equals(signature(mismatch)) ? candidate : input; }
    }

    private String failureSignature(FuzzInput input) throws Exception {
        try { check.check(input); return null; }
        catch (DifferentialMismatch mismatch) { return signature(mismatch); }
    }

    private String signature(DifferentialMismatch mismatch) {
        return mismatch.getMessage().split(":", 2)[0];
    }
}
