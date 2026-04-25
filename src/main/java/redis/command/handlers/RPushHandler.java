package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class RPushHandler implements CommandHandler {

    private final DataStore store;

    public RPushHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.size() < 2) {
            return RespWriter.integer(0);
        }
        var list = store.addToList(args.getFirst(), args.subList(1, args.size()));
        return RespWriter.integer(list.size());
    }
}
