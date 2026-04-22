package redis;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespParser {

    private static final String CRLF = "\r\n";

    public RespParser() {
    }

    public RespDataType getDataType(byte[] bytes) {
        var firstChar = (char) bytes[0];
        return RespDataType.fromPrefix(firstChar + "");
    }

    public static String getSimpleString(byte[] bytes) {
        return new String(bytes, 1, bytes.length - 2);
    }

    public static String getError(byte[] bytes) {
        return new String(bytes, 1, bytes.length - 2);
    }

    /**
     * Parses a RESP2 bulk string: type {@code $}, length line, then payload and trailing CRLF.
     * A length of -1 (Redis null bulk string) yields {@code null}.
     */
    public static String getBulkString(byte[] bytes) {
        if (bytes == null || bytes.length < 4) {
            return null;
        }
        int i = 1; // first byte after '$'
        while (i < bytes.length && bytes[i] != '\r') {
            i++;
        }
        if (i + 1 >= bytes.length || bytes[i] != '\r' || bytes[i + 1] != '\n') {
            return null;
        }
        String lenStr = new String(bytes, 1, i - 1, StandardCharsets.UTF_8);
        final int bulkLen;
        try {
            bulkLen = Integer.parseInt(lenStr);
        } catch (NumberFormatException e) {
            return null;
        }
        if (bulkLen == -1) {
            return null;
        }
        if (bulkLen < 0) {
            return null;
        }
        int payloadStart = i + 2;
        if (payloadStart + bulkLen + 2 > bytes.length) {
            return null;
        }
        if (bytes[payloadStart + bulkLen] != '\r' || bytes[payloadStart + bulkLen + 1] != '\n') {
            return null;
        }
        return new String(bytes, payloadStart, bulkLen, StandardCharsets.UTF_8);
    }

    public static long getInteger(byte[] bytes) {
        return Long.parseLong(new String(bytes, 1, bytes.length - 2, StandardCharsets.UTF_8));
    }

    public static List<RespDataHolder<?>> parseArray(byte[] bytes) {
        List<RespDataHolder<?>> elements = new ArrayList<>();
        int i = 1;
        int count = 0;
        while (bytes[i] >= '0' && bytes[i] <= '9') {
            count = count * 10 + (bytes[i] - '0');
            i++;
        }
        i += 2; // \r\n
        for (int e = 0; e < count; e++) {
            if (bytes[i] == ':') {
                int v0 = i + 1;
                i = v0;
                while (bytes[i] != '\r') {
                    i++;
                }
                long v = Long.parseLong(new String(bytes, v0, i - v0, StandardCharsets.UTF_8));
                elements.add(new RespDataHolder<>(RespDataType.INTEGER, v));
                i += 2;
            } else if (bytes[i] == '+') {
                int s0 = i + 1;
                i = s0;
                while (bytes[i] != '\r') {
                    i++;
                }
                String s = new String(bytes, s0, i - s0, StandardCharsets.UTF_8);
                elements.add(new RespDataHolder<>(RespDataType.SIMPLE_STRING, s));
                i += 2;
            } else { // bulk string: $ <len> \r\n <payload> \r\n
                int l0 = i + 1;
                i = l0;
                while (bytes[i] != '\r') {
                    i++;
                }
                int blen = Integer.parseInt(new String(bytes, l0, i - l0, StandardCharsets.UTF_8));
                i += 2;
                if (blen == -1) {
                    elements.add(new RespDataHolder<>(RespDataType.BULK_STRING, (String) null));
                } else {
                    String s = new String(bytes, i, blen, StandardCharsets.UTF_8);
                    elements.add(new RespDataHolder<>(RespDataType.BULK_STRING, s));
                    i += blen + 2;
                }
            }
        }
        return elements;
    }

}
