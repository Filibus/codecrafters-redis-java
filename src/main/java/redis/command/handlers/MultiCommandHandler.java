package redis.command.handlers;

import java.util.List;
import redis.command.Command;
import redis.command.CommandHandler;
import redis.domain.StreamId;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class MultiCommandHandler implements CommandHandler {

    private final DataStore store;

    public MultiCommandHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        store.addConnection(connectionId);
        return RespWriter.simpleString("OK");
    }

}
