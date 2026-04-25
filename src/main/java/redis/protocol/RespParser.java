package redis.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import redis.command.Command;

/**
 * Parses inbound RESP2 bulk-string array commands.
 */
public final class RespParser {

    private RespParser() {
    }

    public static Optional<Command> parseCommand(byte[] bytes) {
        if (bytes == null || bytes.length < 4 || bytes[0] != '*') {
            return Optional.empty();
        }
        List<String> array = parseBulkStringArray(bytes);
        if (array.isEmpty()) {
            return Optional.empty();
        }
        String name = array.getFirst();
        List<String> args = array.size() == 1 ? List.of() : array.subList(1, array.size());
        return Optional.of(new Command(name, args));
    }

    /**
     * Parses a RESP2 array: {@code *<n>\r\n} followed by element objects.
     * Elements may be simple strings, integers, or bulk strings (codecrafters uses bulk for commands).
     */
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private static List<String> parseBulkStringArray(byte[] bytes) {
        List<String> out = new ArrayList<>();
        int i = 1;
        int count = 0;
        while (i < bytes.length && bytes[i] >= '0' && bytes[i] <= '9') {
            count = count * 10 + (bytes[i] - '0');
            i++;
        }
        i += 2; // \r\n
        for (int e = 0; e < count; e++) {
            if (i >= bytes.length) {
                return List.of();
            }
            if (bytes[i] == ':') {
                int v0 = i + 1;
                i = v0;
                while (i < bytes.length && bytes[i] != '\r') {
                    i++;
                }
                if (i >= bytes.length) {
                    return List.of();
                }
                long v = Long.parseLong(new String(bytes, v0, i - v0, StandardCharsets.UTF_8));
                out.add(Long.toString(v));
                i += 2;
            } else if (bytes[i] == '+') {
                int s0 = i + 1;
                i = s0;
                while (i < bytes.length && bytes[i] != '\r') {
                    i++;
                }
                if (i >= bytes.length) {
                    return List.of();
                }
                String s = new String(bytes, s0, i - s0, StandardCharsets.UTF_8);
                out.add(s);
                i += 2;
            } else { // bulk string
                int l0 = i + 1;
                i = l0;
                while (i < bytes.length && bytes[i] != '\r') {
                    i++;
                }
                if (i >= bytes.length) {
                    return List.of();
                }
                int blen = Integer.parseInt(new String(bytes, l0, i - l0, StandardCharsets.UTF_8));
                i += 2;
                if (blen == -1) {
                    out.add(null);
                } else {
                    if (i + blen + 1 >= bytes.length) {
                        return List.of();
                    }
                    String s = new String(bytes, i, blen, StandardCharsets.UTF_8);
                    out.add(s);
                    i += blen + 2;
                }
            }
        }
        return out;
    }
}
