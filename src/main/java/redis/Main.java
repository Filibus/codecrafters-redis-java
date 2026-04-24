package redis;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class Main {

    private static final String CRLF = "\r\n";
    private static final String NULL_ARRAY = "*-1\r\n";
    static final RedisInMemory redisData = new RedisInMemory();

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
                        int bytesRead;
                        while ((bytesRead = inputStream.read(buffer)) != -1) {
                            byte[] request = Arrays.copyOf(buffer, bytesRead);
                            var respString = getRespString(request);
                            if (respString != null) {
                                clientSocket.getOutputStream().write(respString.getBytes(StandardCharsets.UTF_8));
                            }
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
        return switch (bytes[0]) {
            case '+' -> "+" + RespParser.getSimpleString(bytes) + CRLF;
            case '*' -> handleCommand(parseCommand(bytes));
            default -> null;
        };
    }

    private static String handleCommand(RespCommand command) {
        if (command == null || command.command() == null) {
            return null;
        }

        String cmd = command.command().toUpperCase(Locale.ROOT);
        return switch (cmd) {
            case "PING" -> "+PONG\r\n";
            case "ECHO" -> handleEcho(command.args());
            case "SET" -> handleSet(command.args());
            case "TYPE" -> handleType(command.args());
            case "GET" -> handleGet(command.args());
            case "RPUSH" -> handleRpush(command.args());
            case "LPUSH" -> handleLpush(command.args());
            case "LRANGE" -> handleLrange(command.args());
            case "LLEN" -> handleLlen(command.args());
            case "LPOP" -> handleLpop(command.args());
            case "BLPOP" -> handleBlpop(command.args());
            case "XADD" -> handleXadd(command.args());
            case "XRANGE" -> handleXrange(command.args());
            case "XREAD" -> handleXread(command.args());
            default -> null;
        };
    }

    private static String handleEcho(List<String> args) {
        if (args.isEmpty()) {
            return deserializeString("");
        }
        return deserializeString(args.getFirst());
    }

    private static String handleSet(List<String> args) {
        if (args.size() < 2) {
            return deserializeString(null);
        }
        String key = args.getFirst();
        String value = args.get(1);
        redisData.set(key, value, getTTL(args).orElse(null));
        return "+OK\r\n";
    }

    private static String handleType(List<String> args) {
        if (args.isEmpty()) {
            return "+none\r\n";
        }
        return "+" + redisData.getType(args.getFirst()) + CRLF;
    }

    private static String handleGet(List<String> args) {
        if (args.isEmpty()) {
            return deserializeString(null);
        }
        return redisData.getIfPresent(args.getFirst())
                .map(Entry::value)
                .map(Main::deserializeString)
                .orElse("$-1\r\n");
    }

    private static String handleRpush(List<String> args) {
        if (args.size() < 2) {
            return ":0\r\n";
        }
        var list = redisData.addToList(args.getFirst(), args.subList(1, args.size()));
        return ":" + list.size() + CRLF;
    }

    private static String handleLpush(List<String> args) {
        if (args.size() < 2) {
            return ":0\r\n";
        }
        var list = redisData.prependToList(args.getFirst(), args.subList(1, args.size()));
        return ":" + list.size() + CRLF;
    }

    private static String handleLrange(List<String> args) {
        if (args.size() < 3) {
            return "*0\r\n";
        }
        String key = args.getFirst();
        int start = Integer.parseInt(args.get(1));
        int stop = Integer.parseInt(args.get(2));
        return deserializeList(redisData.lRange(key, start, stop));
    }

    private static String handleLlen(List<String> args) {
        if (args.isEmpty()) {
            return ":0\r\n";
        }
        return ":" + redisData.getListSize(args.getFirst()) + CRLF;
    }

    private static String handleLpop(List<String> args) {
        if (args.isEmpty()) {
            return deserializeString(null);
        }

        String key = args.getFirst();
        if (args.size() == 1) {
            Entry popped = redisData.popElement(key);
            return deserializeString(popped == null ? null : popped.value());
        }

        List<Entry> poppedElements = redisData.popElements(key, Integer.parseInt(args.get(1)));
        if (poppedElements == null) {
            return NULL_ARRAY;
        }
        return deserializeArray(poppedElements.stream().map(Entry::value).toList());
    }

    private static String handleBlpop(List<String> args) {
        if (args.size() < 2) {
            return NULL_ARRAY;
        }

        String listName = args.getFirst();
        long millisToWait = new BigDecimal(args.get(1)).multiply(new BigDecimal(1000)).longValue();
        Entry poppedElement = redisData.blPop(listName, millisToWait);
        if (poppedElement == null) {
            return NULL_ARRAY;
        }
        return deserializeArray(List.of(listName, poppedElement.value()));
    }

    private static String handleXadd(List<String> args) {
        if (args.size() < 4) {
            return deserializeString(null);
        }
        String streamKey = args.getFirst();
        String entryId = args.get(1);
        List<String> keyValuePairs = args.subList(2, args.size());
        try {
            StreamId addedId = redisData.xAdd(streamKey, entryId, keyValuePairs);
            return deserializeString(addedId.toString());
        } catch (IllegalArgumentException ex) {
            return deserializeError(ex);
        }
    }

    private static String handleXrange(List<String> args) {
        if (args.size() < 3) {
            return deserializeString(null);
        }
        try {
            String streamKey = args.getFirst();
            String startId = args.get(1);
            String endId = args.get(2);
            return deserializeStream(redisData.rangeStreamEntries(streamKey, startId, endId));
        } catch (IllegalArgumentException ex) {
            return deserializeError(ex);
        }
    }

    private static String handleXread(List<String> args) {
        if (args.isEmpty()) {
            return syntaxError();
        }

        Long blockMillis = null;
        int cursor = 0;
        if (args.get(0).equalsIgnoreCase("BLOCK")) {
            if (args.size() < 2) {
                return syntaxError();
            }
            try {
                blockMillis = Long.parseLong(args.get(1));
            } catch (NumberFormatException e) {
                return syntaxError();
            }
            if (blockMillis < 0) {
                return syntaxError();
            }
            cursor = 2;
        }

        if (cursor >= args.size() || !args.get(cursor).equalsIgnoreCase("STREAMS")) {
            return syntaxError();
        }
        cursor++;

        List<String> rest = args.subList(cursor, args.size());
        if (rest.isEmpty() || rest.size() % 2 != 0) {
            return syntaxError();
        }
        int half = rest.size() / 2;
        List<String> streamKeys = rest.subList(0, half);
        List<String> streamIds = rest.subList(half, rest.size());

        LinkedHashMap<String, StreamId> startAfterIds = resolveStartIds(streamKeys, streamIds);

        if (blockMillis == null) {
            return readStreamsNonBlocking(startAfterIds);
        }
        return readStreamsBlocking(startAfterIds, blockMillis);
    }

    private static LinkedHashMap<String, StreamId> resolveStartIds(
            List<String> streamKeys, List<String> streamIds) {

        LinkedHashMap<String, StreamId> startAfterIds = new LinkedHashMap<>();
        for (int i = 0; i < streamKeys.size(); i++) {
            String key = streamKeys.get(i);
            String rawId = streamIds.get(i);
            StreamId start = "$".equals(rawId)
                    ? redisData.lastStreamId(key)
                    : StreamId.fromRange(rawId);
            startAfterIds.put(key, start);
        }
        return startAfterIds;
    }

    private static String readStreamsNonBlocking(LinkedHashMap<String, StreamId> startAfterIds) {
        LinkedHashMap<String, List<StreamEntry>> streamEntries = new LinkedHashMap<>();
        for (Map.Entry<String, StreamId> e : startAfterIds.entrySet()) {
            List<StreamEntry> entries = redisData.xRead(e.getKey(), e.getValue().toString());
            if (!entries.isEmpty()) {
                streamEntries.put(e.getKey(), entries);
            }
        }
        if (streamEntries.isEmpty()) {
            return NULL_ARRAY;
        }
        return deserializeReadStream(streamEntries);
    }

    private static String readStreamsBlocking(
            LinkedHashMap<String, StreamId> startAfterIds, long blockMillis) {

        Map<String, List<StreamEntry>> result = redisData.xReadBlocking(startAfterIds, blockMillis);
        if (result == null || result.isEmpty()) {
            return NULL_ARRAY;
        }
        return deserializeReadStream(result);
    }

    private static String syntaxError() {
        return "-ERR syntax error\r\n";
    }

    private static RespCommand parseCommand(byte[] bytes) {
        List<RespDataHolder<?>> array = RespParser.parseArray(bytes);
        if (array.isEmpty()) {
            return null;
        }
        if (array.getFirst().dataType() == RespDataType.BULK_STRING) {
            if (array.size() == 1) {
                return new RespCommand((String) array.getFirst().data(), Collections.emptyList());
            }
            List<String> args = array.subList(1, array.size())
                    .stream().map(ar -> (String) ar.data()).toList();
            return new RespCommand((String) array.getFirst().data(), args);
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
            return "*-1\r\n";
        }
        return "*" + items.size() + "\r\n"
                + items.stream().map(item -> "$" + item.length() + "\r\n" + item + "\r\n")
                .collect(Collectors.joining());
    }

    private static String deserializeReadStream(Map<String,List<StreamEntry>> streamEntries) {
        if (streamEntries == null || streamEntries.isEmpty()) {
            return "*-1\r\n";
        }
        StringBuilder result = new StringBuilder(64 + streamEntries.size() * 48);
        result.append('*').append(streamEntries.size()).append("\r\n");
        for (Map.Entry<String, List<StreamEntry>> streamEntry : streamEntries.entrySet()) {
            result.append("*2\r\n");
            appendBulkString(result, streamEntry.getKey());
            appendStreamEntries(result, streamEntry.getValue());
        }
        return result.toString();
    }

    private static String deserializeStream(List<StreamEntry> streamEntries) {
        if (streamEntries == null || streamEntries.isEmpty()) {
            return "*-1\r\n";
        }
        StringBuilder result = new StringBuilder(64 + streamEntries.size() * 64);
        appendStreamEntries(result, streamEntries);
        return result.toString();
    }

    private static void appendStreamEntries(StringBuilder result, List<StreamEntry> streamEntries) {
        if (streamEntries == null || streamEntries.isEmpty()) {
            result.append("*-1\r\n");
            return;
        }
        result.append('*').append(streamEntries.size()).append("\r\n");
        for (StreamEntry entry : streamEntries) {
            result.append("*2\r\n");
            appendBulkString(result, entry.id().toString());
            appendFields(result, entry.fields());
        }
    }

    private static void appendFields(StringBuilder result, Map<String, String> fields) {
        result.append('*').append(fields.size() * 2).append("\r\n");
        for (Map.Entry<String, String> field : fields.entrySet()) {
            appendBulkString(result, field.getKey());
            appendBulkString(result, field.getValue());
        }
    }

    private static void appendBulkString(StringBuilder result, String value) {
        if (value == null) {
            result.append("$-1\r\n");
            return;
        }
        result.append('$')
                .append(value.length())
                .append("\r\n")
                .append(value)
                .append("\r\n");
    }

    private static String deserializeError(Exception ex) {
        return "-ERR " + ex.getMessage() + "\r\n";
    }
}

