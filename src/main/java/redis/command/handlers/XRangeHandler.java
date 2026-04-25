package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class XRangeHandler implements CommandHandler {

    private final DataStore store;

    public XRangeHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() < 3) {
            return RespWriter.bulkString(null);
        }
        try {
            String streamKey = args.getFirst();
            String startId = args.get(1);
            String endId = args.get(2);
            return RespWriter.streamEntryList(store.rangeStreamEntries(streamKey, startId, endId));
        } catch (IllegalArgumentException ex) {
            return RespWriter.error(ex.getMessage());
        }
    }
}
