package redis.command;

import java.util.List;

@FunctionalInterface
public interface CommandHandler {
    String execute(List<String> args, String connectionId);
}
