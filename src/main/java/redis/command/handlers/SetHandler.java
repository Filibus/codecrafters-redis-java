package redis.command.handlers;

import java.util.List;
import java.util.Optional;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class SetHandler implements CommandHandler {

    private final DataStore store;

    public SetHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.size() < 2) {
            return RespWriter.bulkString(null);
        }
        String key = args.getFirst();
        String value = args.get(1);
        store.set(key, value, getTtl(args).orElse(null));
        return RespWriter.OK;
    }

    private static Optional<Long> getTtl(List<String> args) {
        if (args.size() >= 4) {
            if (args.get(2).equalsIgnoreCase("EX")) {
                return Optional.of(Long.parseLong(args.get(3)) * 1000);
            } else if (args.get(2).equalsIgnoreCase("PX")) {
                return Optional.of(Long.parseLong(args.get(3)));
            }
            return Optional.empty();
        }
        return Optional.empty();
    }
}
