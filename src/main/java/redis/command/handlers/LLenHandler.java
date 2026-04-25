package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class LLenHandler implements CommandHandler {

    private final DataStore store;

    public LLenHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.isEmpty()) {
            return RespWriter.integer(0);
        }
        return RespWriter.integer(store.getListSize(args.getFirst()));
    }
}
