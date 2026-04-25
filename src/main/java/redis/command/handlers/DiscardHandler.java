package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class DiscardHandler implements CommandHandler {

    private final DataStore store;

    public DiscardHandler(
            DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (!store.connectionIsOpen(connectionId)) {
            return RespWriter.error("DISCARD without MULTI");
        }
        store.resetConnection(connectionId);
        store.unwatch(connectionId);
        return RespWriter.OK;
    }
}
