package redis.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import redis.command.Command;
import redis.domain.Entry;
import redis.domain.RedisType;
import redis.domain.StreamEntry;
import redis.domain.StreamId;
import redis.protocol.RespWriter;

/**
 * Facade for string, list, and stream storage with a single type per key, mirroring the
 * previous {@code RedisInMemory} behavior.
 */
public final class DataStore {

    private final Object storeLock = new Object();
    private final KeyTypeRegistry types = new KeyTypeRegistry();
    private final StringStore strings = new StringStore(types);
    private final ListStore lists = new ListStore();
    private final StreamStore streams = new StreamStore();
    private final MultiCommandStore commandStore = new MultiCommandStore();

    /** Logical version per key; any mutation advances it (used for WATCH/EXEC). */
    private final Map<String, Long> keyVersions = new ConcurrentHashMap<>();

    private final Map<String, Map<String, Long>> connectionWatches = new HashMap<>();

    public void assignType(String key, RedisType type) {
        synchronized (storeLock) {
            assignTypeLocked(key, type);
        }
    }

    private void assignTypeLocked(String key, RedisType type) {
        types.put(key, type);
        switch (type) {
            case STRING -> {
                lists.clear(key);
                streams.clear(key);
            }
            case LIST -> {
                strings.clear(key);
                streams.clear(key);
            }
            case STREAM -> {
                strings.clear(key);
                lists.clear(key);
            }
            case NONE -> {
                strings.clear(key);
                lists.clear(key);
                streams.clear(key);
            }
        }
    }

    public void set(String key, String value, Long ttlMillis) {
        synchronized (storeLock) {
            assignTypeLocked(key, RedisType.STRING);
            strings.set(key, value, ttlMillis);
            bumpKeyVersion(key);
        }
    }

    public String getType(String key) {
        synchronized (storeLock) {
            return types.getOrDefault(key).wireValue();
        }
    }

    public Optional<Entry> getIfPresent(String key) {
        synchronized (storeLock) {
            return strings.getIfPresent(key);
        }
    }

    public List<Entry> addToList(String key, List<String> values) {
        synchronized (storeLock) {
            assignTypeLocked(key, RedisType.LIST);
            List<Entry> result = lists.addToList(key, values);
            bumpKeyVersion(key);
            return result;
        }
    }

    public List<Entry> prependToList(String key, List<String> values) {
        synchronized (storeLock) {
            assignTypeLocked(key, RedisType.LIST);
            List<Entry> result = lists.prependToList(key, values);
            bumpKeyVersion(key);
            return result;
        }
    }

    public Entry popElement(String key) {
        synchronized (storeLock) {
            Entry popped = lists.popElement(key);
            if (popped != null) {
                bumpKeyVersion(key);
            }
            return popped;
        }
    }

    public void addConnection(String connectionId) {
        synchronized (storeLock) {
            commandStore.addConnection(connectionId);
        }
    }

    public boolean connectionIsOpen(String connectionId) {
        synchronized (storeLock) {
            return commandStore.connectionOpen(connectionId);
        }
    }

    public void resetConnection(String connectionId) {
        synchronized (storeLock) {
            commandStore.resetConnection(connectionId);
        }
    }

    public String addCommand(String connectionId, Command command) {
        synchronized (storeLock) {
            commandStore.addCommand(connectionId, command);
            return RespWriter.simpleString("QUEUED");
        }
    }

    public List<Command> getCommands(String connectionId) {
        synchronized (storeLock) {
            return commandStore.getCommand(connectionId);
        }
    }

    public List<Command> removeCommands(String connectionId) {
        synchronized (storeLock) {
            return commandStore.removeCommands(connectionId);
        }
    }

    public List<Entry> popElements(String key, int count) {
        synchronized (storeLock) {
            List<Entry> popped = lists.popElements(key, count);
            if (!popped.isEmpty()) {
                bumpKeyVersion(key);
            }
            return popped;
        }
    }

    public Long increment(String key) {
        synchronized (storeLock) {
            Long n = strings.increment(key);
            bumpKeyVersion(key);
            return n;
        }
    }

    public Entry bLPop(String key, long timeoutMillis) {
        Entry e = lists.bLPop(key, timeoutMillis);
        if (e != null) {
            bumpKeyVersion(key);
        }
        return e;
    }

    public int getListSize(String key) {
        synchronized (storeLock) {
            return lists.getListSize(key);
        }
    }

    public List<String> lRange(String key, int start, int stop) {
        synchronized (storeLock) {
            return lists.lRange(key, start, stop);
        }
    }

    public StreamId xAdd(String key, String id, List<String> keyValuePairs) {
        synchronized (storeLock) {
            assignTypeLocked(key, RedisType.STREAM);
            StreamId idOut = streams.xAdd(key, id, keyValuePairs);
            bumpKeyVersion(key);
            return idOut;
        }
    }

    public List<StreamEntry> rangeStreamEntries(String key, String startId, String endId) {
        synchronized (storeLock) {
            return streams.rangeStreamEntries(key, startId, endId);
        }
    }

    public List<StreamEntry> xRead(String key, String startId) {
        synchronized (storeLock) {
            return streams.xRead(key, startId);
        }
    }

    public StreamId lastStreamId(String key) {
        synchronized (storeLock) {
            return streams.lastStreamId(key);
        }
    }

    public Map<String, List<StreamEntry>> xReadBlocking(
            LinkedHashMap<String, StreamId> startAfterIds, Long timeoutMillis) {
        return streams.xReadBlocking(storeLock, startAfterIds, timeoutMillis);
    }

    public void watch(String connectionId, List<String> keys) {
        synchronized (storeLock) {
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
    }

    /** Clears all WATCHed keys for this connection (UNWATCH). */
    public void unwatch(String connectionId) {
        synchronized (storeLock) {
            unwatchConnectionLocked(connectionId);
        }
    }

    public String runExec(
            String connectionId, BiFunction<Command, String, String> runEachCommand) {
        synchronized (storeLock) {
            if (!commandStore.connectionOpen(connectionId)) {
                return null;
            }
            List<Command> commands = new ArrayList<>(commandStore.getCommand(connectionId));
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

    private void bumpKeyVersion(String key) {
        keyVersions.merge(key, 1L, Long::sum);
    }
}
