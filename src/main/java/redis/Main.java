package redis;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    static final String CLRF = "\r\n";

    public static void main(String[] args) {
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            while (true) {
                System.out.println("Waiting for connection...");
                final Socket clientSocket = serverSocket.accept();
                Thread.ofVirtual().start(() -> {
                    try (clientSocket) {
                        System.out.println("Connected to client");
                        InputStream inputStream = clientSocket.getInputStream();
                        byte[] buffer = new byte[1024];
                        while (inputStream.read(buffer) != -1) {
                            clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
                        }
                    } catch (IOException e) {
                        System.out.println("Error handling client: " + e.getMessage());
                    }
                });
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
