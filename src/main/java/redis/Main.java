package redis;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
                            var respString = getRespString(buffer);
                            clientSocket.getOutputStream().write(respString.getBytes());
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

    private static String getRespString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        if (bytes[0] == '+') {
            var respString = RespParser.getSimpleString(bytes);
            return "+" + respString + "\r\n";
        } else if (bytes[0] == '*') {
            var command = parseCommand(bytes);
            if(command == null) return null;
            if (command.getCommand().equalsIgnoreCase("PING")) {
                return "+PONG\r\n";
            } else if (command.getCommand().equalsIgnoreCase("ECHO")) {
                var commandArg = command.getArgs().getFirst();
                return "$" + commandArg.length() + "\r\n" + commandArg + "\r\n";
            }
        }
        return null;
    }

    private static RespCommand parseCommand(byte[] bytes) {
        List<RespDataHolder<?>> array = RespParser.parseArray(bytes);
        if (array.get(0).getDataType() == RespDataType.BULK_STRING) {
            if (array.size() != 2) {
                return new RespCommand((String) array.getFirst().data, Collections.emptyList());
            }
            return new RespCommand((String) array.get(0).data, List.of((String) array.get(1).data));
        }
        return null;
    }
}
