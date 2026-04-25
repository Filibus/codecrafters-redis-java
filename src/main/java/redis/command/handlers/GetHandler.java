package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.domain.Entry;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class GetHandler implements CommandHandler {

    private final DataStore store;

    public GetHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args) {
        if (args.isEmpty()) {
            return RespWriter.bulkString(null);
        }
        return store.getIfPresent(args.getFirst())
                .map(Entry::value)
                .map(RespWriter::bulkString)
                .orElse(RespWriter.NULL_BULK);
    }
}
