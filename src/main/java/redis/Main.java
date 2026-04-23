package redis;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Main {

    static final RedisInMemory redisData = new RedisInMemory();

    static void main(String[] args) {
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
            if (command == null) return null;
            if (command.getCommand().equalsIgnoreCase("PING")) {
                return "+PONG\r\n";
            } else if (command.getCommand().equalsIgnoreCase("ECHO")) {
                var commandArg = command.getArgs().getFirst();
                return "$" + commandArg.length() + "\r\n" + commandArg + "\r\n";
            } else if (command.getCommand().equalsIgnoreCase("SET")) {
                var commandKey = command.getArgs().getFirst();
                var commandValue = command.getArgs().get(1);
                var expiresAt = getTTL(command.getArgs());
                redisData.set(commandKey, commandValue, expiresAt.orElse(null));
                return "+OK\r\n";
            } else if (command.getCommand().equalsIgnoreCase("GET")) {
                var commandKey = command.getArgs().getFirst();
                var redisValue = redisData.getIfPresent(commandKey);
                return redisValue.map(entry -> "$" + entry.value().length()
                                + "\r\n" + entry.value() + "\r\n")
                        .orElse("$-1\r\n");
            } else if ("RPUSH".equalsIgnoreCase(command.getCommand())) {
                var commandKey = command.getArgs().getFirst();
                var commandValue = command.getArgs().subList(1, command.getArgs().size());
                var list = redisData.addToList(commandKey, commandValue);
                return ":" + list.size() + "\r\n";
            } else if ("LPUSH".equalsIgnoreCase(command.getCommand())) {
                var commandKey = command.getArgs().getFirst();
                var commandValue = command.getArgs().subList(1, command.getArgs().size());
                var list = redisData.prependToList(commandKey, commandValue);
                return ":" + list.size() + "\r\n";
            } else if ("LRANGE".equalsIgnoreCase(command.getCommand())) {
                var commandKey = command.getArgs().getFirst();
                var start = Integer.parseInt(command.getArgs().get(1));
                var stop = Integer.parseInt(command.getArgs().get(2));
                var list = redisData.lRange(commandKey, start, stop);
                return deserializeList(list);
            } else if ("LLEN".equalsIgnoreCase(command.getCommand())) {
                var commandKey = command.getArgs().getFirst();
                var listSize = redisData.getListSize(commandKey);
                return ":" + listSize + "\r\n";
            } else if ("LPOP".equalsIgnoreCase(command.getCommand())) {
                var commandKey = command.getArgs().getFirst();
                var args = command.getArgs();
                if (args.size() == 1) {
                    var poppedElement = redisData.popEelement(commandKey).value();
                    return deserializeString(poppedElement);
                }
                var poppedElements = redisData.popEelements(commandKey, Integer.valueOf(args.get(1)))
                        .stream().map(RedisInMemory.Entry::value).toList();
                return deserializeArray(poppedElements);
            }
        }
        return null;
    }

    private static RespCommand parseCommand(byte[] bytes) {
        List<RespDataHolder<?>> array = RespParser.parseArray(bytes);
        if (array.get(0).getDataType() == RespDataType.BULK_STRING) {
            if (array.size() == 1) {
                return new RespCommand((String) array.getFirst().data, Collections.emptyList());
            }
            List<String> args = array.subList(1, array.size())
                    .stream().map(ar -> (String) ar.data).toList();
            return new RespCommand((String) array.getFirst().data, args);
        }
        return null;
    }

    private static Optional<Long> getTTL(List<String> args) {
        if (args.size() >= 4) {
            if (args.get(2).equalsIgnoreCase("EX")) {
                return Optional.of(Long.parseLong(args.get(3)) * 1000);
            } else if (args.get(2).equalsIgnoreCase("PX")) {
                return Optional.of(Long.parseLong(args.get(3)));
            }
            return Optional.empty();
        }
        return Optional.empty();
    }

    private static String deserializeList(List<String> items) {
        if (items == null || items.isEmpty()) {
            return "*0\r\n";
        }
        StringBuilder result = new StringBuilder();
        result.append("*").append(items.size()).append("\r\n");
        for (String item : items) {
            result.append("$").append(item.length()).append("\r\n");
            result.append(item).append("\r\n");
        }
        return result.toString();
    }

    private static String deserializeString(String item) {
        if (item == null) {
            return "$-1\r\n";
        }
        return "$" + item.length() + "\r\n" + item + "\r\n";
    }

    private static String deserializeArray(List<String> items) {
        if (items == null) {
            return "$-1\r\n";
        }
        return "*" + items.size() + "\r\n"
                + items.stream().map(item -> "$" + item.length() + "\r\n" + item + "\r\n")
                .collect(Collectors.joining());
    }
}

