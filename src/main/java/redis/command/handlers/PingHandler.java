package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;

public final class PingHandler implements CommandHandler {
    @Override
    public String execute(List<String> args, String connectionId) {
        return RespWriter.PONG;
    }
}
