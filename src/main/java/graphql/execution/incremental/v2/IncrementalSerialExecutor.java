package graphql.execution.incremental.v2;

import graphql.Internal;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;
import java.util.concurrent.CompletableFuture;

@Internal
public final class IncrementalSerialExecutor {
    private final Deque<Runnable> commands = new ArrayDeque<>();
    private final Consumer<Throwable> onError;
    private boolean running;

    public IncrementalSerialExecutor(Consumer<Throwable> onError) {
        this.onError = onError;
    }

    public void execute(Runnable command) {
        synchronized (commands) {
            commands.add(command);
            if (running) return;
            running = true;
        }
        drain();
    }

    private void drain() {
        int processed = 0;
        Runnable command = poll();
        while (command != null) {
            run(command);
            if (++processed == 64) { CompletableFuture.runAsync(this::drain); return; }
            command = poll();
        }
    }

    private Runnable poll() {
        synchronized (commands) {
            Runnable command = commands.poll();
            if (command == null) running = false;
            return command;
        }
    }

    private void run(Runnable command) {
        try { command.run(); }
        catch (Throwable exception) { onError.accept(exception); }
    }
}
