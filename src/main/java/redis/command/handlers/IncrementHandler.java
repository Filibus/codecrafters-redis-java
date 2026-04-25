package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class IncrementHandler implements CommandHandler {

    private final DataStore store;

    public IncrementHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.isEmpty()) {
            return RespWriter.simpleString("none");
        }
        try {
            Long incremented = store.increment(args.getFirst());
            if (incremented == null) {
                return RespWriter.error("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            return RespWriter.integer(incremented);
        } catch (NumberFormatException _) {
            return RespWriter.error("value is not an integer or out of range");
        }
    }
}
