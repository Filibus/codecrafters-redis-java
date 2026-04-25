package redis.command.handlers;

import java.util.List;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class TypeHandler implements CommandHandler {

    private final DataStore store;

    public TypeHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.isEmpty()) {
            return RespWriter.simpleString("none");
        }
        return RespWriter.simpleString(store.getType(args.getFirst()));
    }
}
