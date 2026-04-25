package redis.domain;

import java.util.Map;

public record StreamEntry(StreamId id, Map<String, String> fields) {
}
