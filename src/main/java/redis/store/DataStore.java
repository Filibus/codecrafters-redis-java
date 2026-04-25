package redis.store;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final TransactionState transaction = new TransactionState();

    public void assignType(String key, RedisType type) {
        synchronized (storeLock) {
            assignTypeLocked(key, type);
        }
    }

    private void assignTypeLocked(String key, RedisType type) {
        types.put(key, type);
        if (type != RedisType.STRING) {
            strings.clear(key);
        }
        if (type != RedisType.LIST) {
            lists.clear(key);
        }
        if (type != RedisType.STREAM) {
            streams.clear(key);
        }
    }

    public void set(String key, String value, Long ttlMillis) {
        synchronized (storeLock) {
            assignTypeLocked(key, RedisType.STRING);
            strings.set(key, value, ttlMillis);
            transaction.bumpKeyVersion(key);
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

    public List<Command> getCommands(String connectionId) {
        synchronized (storeLock) {
            return List.copyOf(transaction.copyCommandQueue(connectionId));
        }
    }

    public List<Command> removeCommands(String connectionId) {
        synchronized (storeLock) {
            return transaction.removeCommands(connectionId);
        }
    }

    public List<Entry> addToList(String key, List<String> values) {
        synchronized (storeLock) {
            assignTypeLocked(key, RedisType.LIST);
            List<Entry> result = lists.addToList(key, values);
            transaction.bumpKeyVersion(key);
            return result;
        }
    }

    public List<Entry> prependToList(String key, List<String> values) {
        synchronized (storeLock) {
            assignTypeLocked(key, RedisType.LIST);
            List<Entry> result = lists.prependToList(key, values);
            transaction.bumpKeyVersion(key);
            return result;
        }
    }

    public Entry popElement(String key) {
        synchronized (storeLock) {
            Entry popped = lists.popElement(key);
            if (popped != null) {
                transaction.bumpKeyVersion(key);
            }
            return popped;
        }
    }

    public void addConnection(String connectionId) {
        synchronized (storeLock) {
            transaction.addConnection(connectionId);
        }
    }

    public boolean connectionIsOpen(String connectionId) {
        synchronized (storeLock) {
            return transaction.connectionOpen(connectionId);
        }
    }

    public void resetConnection(String connectionId) {
        synchronized (storeLock) {
            transaction.resetCommandQueue(connectionId);
        }
    }

    public String addCommand(String connectionId, Command command) {
        synchronized (storeLock) {
            transaction.addCommand(connectionId, command);
            return RespWriter.simpleString("QUEUED");
        }
    }

    public List<Entry> popElements(String key, int count) {
        synchronized (storeLock) {
            List<Entry> popped = lists.popElements(key, count);
            if (!popped.isEmpty()) {
                transaction.bumpKeyVersion(key);
            }
            return popped;
        }
    }

    /**
     * @return the new value, or {@code null} if the key is not a string (Redis WRONGTYPE)
     */
    public Long increment(String key) {
        synchronized (storeLock) {
            RedisType t = types.getOrDefault(key);
            if (t != RedisType.NONE && t != RedisType.STRING) {
                return null;
            }
            Long n = strings.increment(key);
            if (t == RedisType.NONE) {
                types.put(key, RedisType.STRING);
            }
            transaction.bumpKeyVersion(key);
            return n;
        }
    }

    public Entry bLPop(String key, long timeoutMillis) {
        Entry e = lists.bLPop(key, timeoutMillis);
        if (e != null) {
            transaction.bumpKeyVersion(key);
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
            transaction.bumpKeyVersion(key);
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
            transaction.watch(connectionId, keys);
        }
    }

    /** Clears all WATCHed keys for this connection (UNWATCH). */
    public void unwatch(String connectionId) {
        synchronized (storeLock) {
            transaction.unwatch(connectionId);
        }
    }

    public String runExec(
            String connectionId, BiFunction<Command, String, String> runEachCommand) {
        synchronized (storeLock) {
            return transaction.runExec(connectionId, runEachCommand);
        }
    }
}
