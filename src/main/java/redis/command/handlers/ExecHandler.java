package redis.command.handlers;

import java.util.List;
import java.util.function.BiFunction;
import redis.command.Command;
import redis.command.CommandHandler;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class ExecHandler implements CommandHandler {

    private final DataStore store;
    private final BiFunction<Command, String, String> runTransactionCommand;

    public ExecHandler(
            DataStore store, BiFunction<Command, String, String> runTransactionCommand) {
        this.store = store;
        this.runTransactionCommand = runTransactionCommand;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (!store.connectionIsOpen(connectionId)) {
            return RespWriter.error("EXEC without MULTI");
        }
        var commands = store.getCommands(connectionId);
        StringBuilder responses = new StringBuilder();
        for (Command c : commands) {
            String response = runTransactionCommand.apply(c, connectionId);
            if (response != null) {
                responses.append(response);
            }
        }
        store.resetConnection(connectionId);
        return "*" + commands.size() + "\r\n" + responses;
    }
}
