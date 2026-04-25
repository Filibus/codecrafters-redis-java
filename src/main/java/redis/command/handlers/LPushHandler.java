package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class LPushHandler implements CommandHandler {

    private final DataStore store;

    public LPushHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.size() < 2) {
            return RespWriter.integer(0);
        }
        var list = store.prependToList(args.getFirst(), args.subList(1, args.size()));
        return RespWriter.integer(list.size());
    }
}
