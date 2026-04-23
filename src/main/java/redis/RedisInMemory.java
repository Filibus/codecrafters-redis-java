package redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RedisInMemory {

    private final Map<String, List<Entry>> redisData = new HashMap<>();

    public record Entry(String value, Long expiresAtMillis) {
        boolean isExpired() {
            return expiresAtMillis != null && System.currentTimeMillis() >= expiresAtMillis;
        }
    }

    public void set(String key, String value) {
        redisData.put(key, List.of(new Entry(value, null)));
    }

    public List<Entry>  addToList(String key, String value) {
        List<Entry> listElements = redisData.get(key);
        if (listElements != null && !listElements.isEmpty()) {
            listElements.add(new Entry(value, null));
            return listElements;
        }
        listElements = new ArrayList<>();
        listElements.add(new Entry(value, null));
        redisData.put(key, listElements);
        return listElements;
    }

    /** Sets a value that expires after {@code ttlMillis} milliseconds. */
    public void set(String key, String value, Long ttlMillis) {
        Long expiresAt = ttlMillis == null || ttlMillis <= 0  ?
               null :  System.currentTimeMillis() + ttlMillis;
        redisData.put(key, List.of(new Entry(value, expiresAt)))    ;
    }

    public Optional<Entry> getIfPresent(String key) {
        List<Entry> e = redisData.get(key);
        if (e == null || e.isEmpty()) {
            return Optional.empty();
        }
        if (e.getFirst().isExpired()) {
            redisData.remove(key);
            return Optional.empty();
        }
        return Optional.of(e.getFirst());
    }
}
