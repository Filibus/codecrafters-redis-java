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
    public String execute(List<String> args) {
        if (args.isEmpty()) {
            return RespWriter.simpleString("none");
        }
        var incremented = store.increment(args.getFirst());
        return RespWriter.integer(incremented);
    }
}
