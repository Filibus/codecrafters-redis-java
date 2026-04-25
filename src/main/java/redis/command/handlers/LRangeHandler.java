package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class LRangeHandler implements CommandHandler {

    private final DataStore store;

    public LRangeHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.size() < 3) {
            return RespWriter.EMPTY_ARRAY;
        }
        String key = args.getFirst();
        int start = Integer.parseInt(args.get(1));
        int stop = Integer.parseInt(args.get(2));
        return RespWriter.stringArrayAsResp(store.lRange(key, start, stop));
    }
}
