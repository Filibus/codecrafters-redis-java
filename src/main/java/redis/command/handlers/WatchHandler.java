package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class WatchHandler implements CommandHandler {

    private final DataStore store;

    public WatchHandler(
            DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        return RespWriter.OK;
    }
}
