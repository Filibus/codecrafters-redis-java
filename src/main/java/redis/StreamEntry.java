package redis;

import java.util.Map;

public record StreamEntry(String id, Map<String, String> fields) {
}
