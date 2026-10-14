package graphql.execution.incremental.v2;

import graphql.Internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Internal
public final class DeferUsageIndex {
    private final Map<DeferUsage, UsageNode> nodes = new LinkedHashMap<>();
    private final List<UsageNode> roots = new ArrayList<>();

    public DeferUsageIndex(CollectedFields fields) {
        fields.getFields().values().forEach(details -> details.forEach(field -> discover(field.getDeferUsage())));
        assignIntervals();
    }

    private void discover(DeferUsage usage) {
        Deque<DeferUsage> stack = new ArrayDeque<>();
        DeferUsage current = usage;
        while (current != null && !nodes.containsKey(current)) {
            stack.push(current);
            current = current.getParent();
        }
        while (!stack.isEmpty()) addNode(stack.pop());
    }

    private void addNode(DeferUsage usage) {
        UsageNode node = new UsageNode();
        nodes.put(usage, node);
        UsageNode parent = nodes.get(usage.getParent());
        if (parent == null) roots.add(node);
        else parent.children.add(node);
    }

    private void assignIntervals() {
        int ordinal = 0;
        for (UsageNode root : roots) ordinal = assignSubtree(root, ordinal);
    }

    private int assignSubtree(UsageNode root, int ordinal) {
        Deque<UsageNode> stack = new ArrayDeque<>();
        stack.push(root);
        root.start = ordinal++;
        while (!stack.isEmpty()) {
            UsageNode node = stack.peek();
            if (node.nextChild == node.children.size()) { node.end = ordinal; stack.pop(); continue; }
            UsageNode child = node.children.get(node.nextChild++);
            child.start = ordinal++;
            stack.push(child);
        }
        return ordinal;
    }

    public void removeDominated(Set<DeferUsage> usages) {
        List<DeferUsage> ordered = new ArrayList<>(usages);
        ordered.sort(Comparator.comparingInt(usage -> nodes.get(usage).start));
        int end = -1;
        for (DeferUsage usage : ordered) {
            UsageNode node = nodes.get(usage);
            if (node.start < end) usages.remove(usage);
            else end = node.end;
        }
    }
}
