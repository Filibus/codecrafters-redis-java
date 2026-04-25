package redis.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import redis.command.CommandRouter;

/**
 * Listens for TCP connections and dispatches one {@link ClientConnection} per socket on a virtual
 * thread.
 */
public final class RedisServer {

    public static final int DEFAULT_PORT = 6379;
    private final int port;
    private final CommandRouter router;

    public RedisServer(int port, CommandRouter router) {
        this.port = port;
        this.router = router;
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            while (true) {
                System.out.println("Waiting for connection...");
                final Socket clientSocket = serverSocket.accept();
                Thread.ofVirtual().start(new ClientConnection(clientSocket, router));
            }
        }
    }
}
