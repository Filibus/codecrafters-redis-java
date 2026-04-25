package redis.command.handlers;

import java.math.BigDecimal;
import java.util.List;
import redis.command.CommandHandler;
import redis.domain.Entry;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class BLPOPHandler implements CommandHandler {

    private final DataStore store;

    public BLPOPHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.size() < 2) {
            return RespWriter.NULL_ARRAY;
        }
        String listName = args.getFirst();
        long millisToWait = new BigDecimal(args.get(1)).multiply(new BigDecimal(1000)).longValue();
        Entry poppedElement = store.bLPop(listName, millisToWait);
        if (poppedElement == null) {
            return RespWriter.NULL_ARRAY;
        }
        return RespWriter.array(List.of(listName, poppedElement.value()));
    }
}
