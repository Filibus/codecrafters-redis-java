package redis.command;

import java.util.List;

public record Command(String name, List<String> args) {
}
