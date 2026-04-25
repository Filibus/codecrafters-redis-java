package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class UnwatchHandler implements CommandHandler {

    private final DataStore store;

    public UnwatchHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        store.unwatch(connectionId);
        return RespWriter.OK;
    }
}
