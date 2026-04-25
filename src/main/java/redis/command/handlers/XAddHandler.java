package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.domain.StreamId;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class XAddHandler implements CommandHandler {

    private final DataStore store;

    public XAddHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.size() < 4) {
            return RespWriter.bulkString(null);
        }
        String streamKey = args.getFirst();
        String entryId = args.get(1);
        List<String> keyValuePairs = args.subList(2, args.size());
        try {
            StreamId addedId = store.xAdd(streamKey, entryId, keyValuePairs);
            return RespWriter.bulkString(addedId.toString());
        } catch (IllegalArgumentException ex) {
            return RespWriter.error(ex.getMessage());
        }
    }
}
