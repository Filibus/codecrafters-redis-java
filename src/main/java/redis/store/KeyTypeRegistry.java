package redis.store;

import java.util.HashMap;
import java.util.Map;
import redis.domain.RedisType;

public final class KeyTypeRegistry {

    private final Map<String, RedisType> types = new HashMap<>();

    public RedisType getOrDefault(String key) {
        return types.getOrDefault(key, RedisType.NONE);
    }

    public void put(String key, RedisType type) {
        if (type == RedisType.NONE) {
            types.remove(key);
        } else {
            types.put(key, type);
        }
    }

    public void remove(String key) {
        types.remove(key);
    }
}
