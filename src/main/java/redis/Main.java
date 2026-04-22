package redis;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    static final String CLRF = "\r\n";

    static void main(String[] args) {
        Socket clientSocket = null;
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            clientSocket = serverSocket.accept();
            InputStream inputStream = clientSocket.getInputStream();
            byte[] buffer = new byte[1024];
            while (inputStream.read(buffer) != -1) {
                clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
            }
        } catch (IOException e) {
            System.out.println("IOEx;jonkn hb,mj ,jmception: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
