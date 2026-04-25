package redis;

import java.io.IOException;
import redis.command.CommandRouter;
import redis.server.RedisServer;
import redis.store.DataStore;

public final class Main {

    public static void main(String[] args) {
        DataStore store = new DataStore();
        CommandRouter router = CommandRouter.withDefaults(store);
        try {
            new RedisServer(RedisServer.DEFAULT_PORT, router).start();
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
