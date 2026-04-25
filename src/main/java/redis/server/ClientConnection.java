package redis.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import redis.command.CommandRouter;
import redis.protocol.RespParser;

/**
 * One virtual-thread per connection: read a frame, parse RESP array, dispatch, write response.
 */
public final class ClientConnection implements Runnable {

    private final Socket clientSocket;
    private final CommandRouter router;
    private final String connectionId = UUID.randomUUID().toString();

    public ClientConnection(Socket clientSocket, CommandRouter router) {
        this.clientSocket = clientSocket;
        this.router = router;
    }

    @Override
    public void run() {
        try (clientSocket) {
            System.out.println("Connected to client with ID: " + connectionId);
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                byte[] request = Arrays.copyOf(buffer, bytesRead);
                RespParser.parseCommand(request)
                        .map(command -> router.dispatch(command, connectionId))
                        .filter(response -> response != null)
                        .ifPresent(
                                response -> {
                                    try {
                                        outputStream.write(
                                                response.getBytes(StandardCharsets.UTF_8)
                                        );
                                    } catch (IOException e) {
                                        System.out.println("Error writing response: " + e.getMessage());
                                    }
                                }
                        );
            }
        } catch (IOException e) {
            System.out.println("Error handling client: " + e.getMessage());
        }
    }
}
