package redis.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import redis.domain.Entry;

public final class StringStore {

    private final Map<String, Entry> stringData = new HashMap<>();
    private final KeyTypeRegistry keyTypes; // for expiry cleanup

    public StringStore(KeyTypeRegistry keyTypes) {
        this.keyTypes = keyTypes;
    }

    public void set(String key, String value, Long ttlMillis) {
        Long expiresAt = ttlMillis == null || ttlMillis <= 0
                ? null
                : System.currentTimeMillis() + ttlMillis;
        stringData.put(key, new Entry(value, expiresAt));
    }

    public Optional<Entry> getIfPresent(String key) {
        Entry e = stringData.get(key);
        if (e == null) {
            return Optional.empty();
        }
        if (e.isExpired()) {
            stringData.remove(key);
            keyTypes.remove(key);
            return Optional.empty();
        }
        return Optional.of(e);
    }

    public void clear(String key) {
        stringData.remove(key);
    }
}
