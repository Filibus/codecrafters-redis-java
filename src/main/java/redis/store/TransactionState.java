package redis.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import redis.command.Command;
import redis.protocol.RespWriter;

/**
 * WATCH/EXEC and MULTI command queue. All methods except {@link #bumpKeyVersion} must be
 * called while holding the same monitor as {@link DataStore}'s store lock. {@code bumpKeyVersion}
 * is thread-safe and may be called from {@link DataStore#bLPop} without that lock.
 */
final class TransactionState {

    private final MultiCommandStore commandStore = new MultiCommandStore();
    private final Map<String, Long> keyVersions = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> connectionWatches = new HashMap<>();

    void addConnection(String connectionId) {
        commandStore.addConnection(connectionId);
    }

    boolean connectionOpen(String connectionId) {
        return commandStore.connectionOpen(connectionId);
    }

    void resetCommandQueue(String connectionId) {
        commandStore.resetConnection(connectionId);
    }

    void addCommand(String connectionId, Command command) {
        commandStore.addCommand(connectionId, command);
    }

    List<Command> removeCommands(String connectionId) {
        return commandStore.removeCommands(connectionId);
    }

    List<Command> copyCommandQueue(String connectionId) {
        return commandStore.copyQueue(connectionId);
    }

    void watch(String connectionId, List<String> keys) {
        if (keys.isEmpty()) {
            unwatchConnectionLocked(connectionId);
            return;
        }
        Map<String, Long> perKey =
                connectionWatches.computeIfAbsent(connectionId, k -> new HashMap<>());
        for (String key : keys) {
            perKey.put(key, keyVersions.getOrDefault(key, 0L));
        }
    }

    void unwatch(String connectionId) {
        unwatchConnectionLocked(connectionId);
    }

    String runExec(String connectionId, BiFunction<Command, String, String> runEachCommand) {
        if (!commandStore.connectionOpen(connectionId)) {
            return null;
        }
        List<Command> commands = commandStore.copyQueue(connectionId);
        if (!watchesSatisfiedLocked(connectionId)) {
            commandStore.resetConnection(connectionId);
            unwatchConnectionLocked(connectionId);
            return RespWriter.NULL_ARRAY;
        }
        if (commands.isEmpty()) {
            commandStore.resetConnection(connectionId);
            unwatchConnectionLocked(connectionId);
            return RespWriter.EMPTY_ARRAY;
        }
        StringBuilder responses = new StringBuilder();
        for (Command c : commands) {
            String response = runEachCommand.apply(c, connectionId);
            if (response != null) {
                responses.append(response);
            }
        }
        commandStore.resetConnection(connectionId);
        unwatchConnectionLocked(connectionId);
        return "*" + commands.size() + "\r\n" + responses;
    }

    void bumpKeyVersion(String key) {
        keyVersions.merge(key, 1L, Long::sum);
    }

    private void unwatchConnectionLocked(String connectionId) {
        connectionWatches.remove(connectionId);
    }

    private boolean watchesSatisfiedLocked(String connectionId) {
        Map<String, Long> watched = connectionWatches.get(connectionId);
        if (watched == null || watched.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, Long> e : watched.entrySet()) {
            long current = keyVersions.getOrDefault(e.getKey(), 0L);
            if (current != e.getValue()) {
                return false;
            }
        }
        return true;
    }
}
