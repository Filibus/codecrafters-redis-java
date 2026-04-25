package redis;

import java.io.IOException;
import redis.command.CommandRouter;
import redis.server.RedisServer;
import redis.store.DataStore;

public final class Main {

    public static void main(String[] args) {
        int port;
        try {
            port = parsePort(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return;
        }

        DataStore store = new DataStore();
        CommandRouter router = CommandRouter.withDefaults(store);
        try {
            new RedisServer(port, router).start();
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static int parsePort(String[] args) {
        int port = RedisServer.DEFAULT_PORT;
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i])) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Error: --port requires a port number");
                }
                String value = args[i + 1];
                int parsed;
                try {
                    parsed = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Error: invalid port: " + value);
                }
                if (parsed < 1 || parsed > 65535) {
                    throw new IllegalArgumentException("Error: port out of range: " + parsed);
                }
                port = parsed;
                i++;
            }
        }
        return port;
    }
}
