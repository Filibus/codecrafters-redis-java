package redis.store;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
            return lists.addToList(key, values);
        }
    }

    public List<Entry> prependToList(String key, List<String> values) {
        synchronized (storeLock) {
            assignTypeLocked(key, RedisType.LIST);
            return lists.prependToList(key, values);
        }
    }

    public Entry popElement(String key) {
        synchronized (storeLock) {
            return lists.popElement(key);
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
            return lists.popElements(key, count);
        }
    }

    public Long increment(String key) {
        synchronized (storeLock) {
            return strings.increment(key);
        }
    }

    public Entry bLPop(String key, long timeoutMillis){
        return lists.bLPop(key, timeoutMillis);
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
            return streams.xAdd(key, id, keyValuePairs);
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
}
