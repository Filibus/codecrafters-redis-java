package redis.command;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import redis.command.handlers.BLPOPHandler;
import redis.command.handlers.DiscardHandler;
import redis.command.handlers.EchoHandler;
import redis.command.handlers.ExecHandler;
import redis.command.handlers.GetHandler;
import redis.command.handlers.IncrementHandler;
import redis.command.handlers.LLenHandler;
import redis.command.handlers.LPopHandler;
import redis.command.handlers.LPushHandler;
import redis.command.handlers.LRangeHandler;
import redis.command.handlers.MultiCommandHandler;
import redis.command.handlers.PingHandler;
import redis.command.handlers.RPushHandler;
import redis.command.handlers.SetHandler;
import redis.command.handlers.TypeHandler;
import redis.command.handlers.WatchHandler;
import redis.command.handlers.XAddHandler;
import redis.command.handlers.XRangeHandler;
import redis.command.handlers.XReadHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class CommandRouter {

    private final Map<String, CommandHandler> handlers = new HashMap<>();
    private final DataStore store;

    public CommandRouter(DataStore store) {
        this.store = store;
        register("PING", new PingHandler());
        register("ECHO", new EchoHandler());
        register("SET", new SetHandler(store));
        register("GET", new GetHandler(store));
        register("TYPE", new TypeHandler(store));
        register("RPUSH", new RPushHandler(store));
        register("LPUSH", new LPushHandler(store));
        register("LRANGE", new LRangeHandler(store));
        register("LLEN", new LLenHandler(store));
        register("LPOP", new LPopHandler(store));
        register("BLPOP", new BLPOPHandler(store));
        register("XADD", new XAddHandler(store));
        register("XRANGE", new XRangeHandler(store));
        register("XREAD", new XReadHandler(store));
        register("INCR", new IncrementHandler(store));
        register("MULTI", new MultiCommandHandler(store));
        register("WATCH", new WatchHandler(store));
        register("DISCARD", new DiscardHandler(store));
        register("EXEC", new ExecHandler(store, this::executeInTransaction));
    }

    public static CommandRouter withDefaults(DataStore store) {
        return new CommandRouter(store);
    }

    private void register(String name, CommandHandler handler) {
        handlers.put(name, handler);
    }

    public String dispatch(Command command, String connectionId) {
        if (command == null || command.name() == null) {
            return null;
        }
        String name = command.name().toUpperCase(Locale.ROOT);
        if (!"EXEC".equals(name) && !"DISCARD".equals(name) && store.connectionIsOpen(connectionId)) {
            if("WATCH".equals(name)){
                return RespWriter.error("WATCH inside MULTI is not allowed");
            }
            return store.addCommand(connectionId, command);
        }
        return executeInTransaction(command, connectionId);
    }

    // Runs a command without MULTI queuing; also used to replay a transaction in EXEC.
    private String executeInTransaction(Command command, String connectionId) {
        String name = command.name().toUpperCase(Locale.ROOT);
        CommandHandler handler = handlers.get(name);
        if (handler == null) {
            return RespWriter.error("unknown command");
        }
        return handler.execute(command.args(), connectionId);
    }
}
