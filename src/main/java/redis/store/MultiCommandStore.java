package redis.store;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import redis.command.Command;

public final class MultiCommandStore {

    private final Map<String, List<Command>> commandData = new ConcurrentHashMap<>();

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

    public List<Command> getCommand(String connectionId) {
        return commandData.computeIfAbsent(connectionId, k -> new LinkedList<>());
    }
}
