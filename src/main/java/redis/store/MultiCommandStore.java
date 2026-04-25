package redis.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import redis.command.Command;

/**
 * Per-connection command queues for {@code MULTI}. Not thread-safe: {@link DataStore} must
 * serialize access (typically via {@code synchronized} on a single store lock).
 */
public final class MultiCommandStore {

    private final Map<String, List<Command>> commandData = new HashMap<>();

    public void addConnection(String connectionId) {
        commandData.put(connectionId, new LinkedList<>());
    }

    public boolean connectionOpen(String connectionId) {
        return commandData.containsKey(connectionId);
    }

    public void resetConnection(String connectionId) {
        commandData.remove(connectionId);
    }

    public List<Command> removeCommands(String connectionId) {
        return commandData.computeIfPresent(connectionId, (k, v) -> {
            v.clear();
            return v;
        });
    }

    public void addCommand(String connectionId, Command command) {
        commandData.computeIfAbsent(connectionId, k -> new LinkedList<>()).add(command);
    }

    /**
     * Snapshot of the queued commands, or an empty list if the connection is not in MULTI. Never
     * creates a new queue entry.
     */
    public List<Command> copyQueue(String connectionId) {
        List<Command> list = commandData.get(connectionId);
        if (list == null) {
            return List.of();
        }
        return new ArrayList<>(list);
    }
}
