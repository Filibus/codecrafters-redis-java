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
        if(store.connectionIsOpen(connectionId)){
            return RespWriter.error("WATCH inside MULTI is not allowed");
        }
        return RespWriter.OK;
    }
}
