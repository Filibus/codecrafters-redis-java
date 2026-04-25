package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.domain.Entry;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class LPopHandler implements CommandHandler {

    private final DataStore store;

    public LPopHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args) {
        if (args.isEmpty()) {
            return RespWriter.bulkString(null);
        }

        String key = args.getFirst();
        if (args.size() == 1) {
            Entry popped = store.popElement(key);
            return RespWriter.bulkString(popped == null ? null : popped.value());
        }

        var poppedElements = store.popElements(key, Integer.parseInt(args.get(1)));
        if (poppedElements == null) {
            return RespWriter.NULL_ARRAY;
        }
        return RespWriter.array(poppedElements.stream().map(Entry::value).toList());
    }
}
