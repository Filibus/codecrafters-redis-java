package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;

public final class EchoHandler implements CommandHandler {
    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.isEmpty()) {
            return RespWriter.bulkString("");
        }
        return RespWriter.bulkString(args.getFirst());
    }
}
