package redis;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RedisInMemory {

    private final Map<String, Entry> redisData = new HashMap<>();

    public record Entry(String value, Long expiresAtMillis) {
        boolean isExpired() {
            return expiresAtMillis != null && System.currentTimeMillis() >= expiresAtMillis;
        }
    }

    public void set(String key, String value) {
        redisData.put(key, new Entry(value, null));
    }

    /** Sets a value that expires after {@code ttlMillis} milliseconds. */
    public void set(String key, String value, Long ttlMillis) {
        Long expiresAt = ttlMillis == null || ttlMillis <= 0  ?
               null :  System.currentTimeMillis() + ttlMillis;
        redisData.put(key, new Entry(value, expiresAt));
    }

    public Optional<Entry> getIfPresent(String key) {
        Entry e = redisData.get(key);
        if (e == null) {
            return Optional.empty();
        }
        if (e.isExpired()) {
            redisData.remove(key);
            return Optional.empty();
        }
        return Optional.of(e);
    }
}
