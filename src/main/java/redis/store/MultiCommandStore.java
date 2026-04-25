package redis.store;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import redis.command.Command;

public final class MultiCommandStore {

    private final Map<String, List<Command>> commandData = new LinkedHashMap<>();

    public void addConnection(String connectionId) {
        commandData.put(connectionId, null);
    }

    public boolean connectionOpen(String connectionId) {
        return commandData.containsKey(connectionId);
    }

    public void addCommand(String connectionId, Command command) {
        commandData.computeIfAbsent(connectionId, k -> new LinkedList<>()).add(command);
    }

    public List<Command> getCommand(String connectionId) {
        return commandData.computeIfAbsent(connectionId, k -> new LinkedList<>());
    }
}
