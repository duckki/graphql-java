package graphql.execution.incremental.v2;

import graphql.Internal;

import java.util.concurrent.CancellationException;

@Internal
public final class IncrementalExecutionControl {
    private volatile boolean stopped;

    public void stop() {
        stopped = true;
    }

    public void checkRunning() {
        if (stopped) throw new CancellationException("Incremental response was closed");
    }
}
