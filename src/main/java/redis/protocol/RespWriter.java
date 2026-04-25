package redis.protocol;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import redis.domain.StreamEntry;

/**
 * Central RESP2 encoding. All wire bytes are built here.
 */
public final class RespWriter {

    public static final String CRLF = "\r\n";
    public static final String NULL_BULK = "$-1\r\n";
    public static final String NULL_ARRAY = "*-1\r\n";
    /** Empty RESP array, used e.g. for LRANGE with wrong arity. */
    public static final String EMPTY_ARRAY = "*0\r\n";
    public static final String OK = "+OK\r\n";
    public static final String QUEUED = "+QUEUED\r\n";
    public static final String PONG = "+PONG\r\n";

    private RespWriter() {
    }

    public static String simpleString(String s) {
        return "+" + s + CRLF;
    }

    public static String error(String message) {
        return "-ERR " + message + CRLF;
    }

    public static String integer(long n) {
        return ":" + n + CRLF;
    }

    public static String bulkString(String item) {
        if (item == null) {
            return NULL_BULK;
        }
        return "$" + item.length() + CRLF + item + CRLF;
    }

    public static String array(List<String> items) {
        if (items == null) {
            return NULL_ARRAY;
        }
        return "*" + items.size() + CRLF
                + items.stream().map(
                        item -> "$" + item.length() + CRLF + item + CRLF
                )
                .collect(Collectors.joining());
    }

    /**
     * LRANGE and similar: empty list is {@code *0}, not a null array.
     */
    public static String stringArrayAsResp(List<String> items) {
        if (items == null || items.isEmpty()) {
            return "*0" + CRLF;
        }
        StringBuilder result = new StringBuilder();
        result.append("*").append(items.size()).append(CRLF);
        for (String item : items) {
            result.append("$").append(item.length()).append(CRLF);
            result.append(item).append(CRLF);
        }
        return result.toString();
    }

    public static String streamEntryList(List<StreamEntry> streamEntries) {
        if (streamEntries == null || streamEntries.isEmpty()) {
            return NULL_ARRAY;
        }
        StringBuilder result = new StringBuilder(64 + streamEntries.size() * 64);
        appendStreamEntryArray(result, streamEntries);
        return result.toString();
    }

    public static String xreadResult(Map<String, List<StreamEntry>> streamEntries) {
        if (streamEntries == null || streamEntries.isEmpty()) {
            return NULL_ARRAY;
        }
        StringBuilder result = new StringBuilder(64 + streamEntries.size() * 48);
        result.append('*').append(streamEntries.size()).append(CRLF);
        for (Map.Entry<String, List<StreamEntry>> streamEntry : streamEntries.entrySet()) {
            result.append("*2\r\n");
            appendBulkString(result, streamEntry.getKey());
            appendStreamEntryArray(result, streamEntry.getValue());
        }
        return result.toString();
    }

    private static void appendStreamEntryArray(StringBuilder result, List<StreamEntry> streamEntries) {
        if (streamEntries == null || streamEntries.isEmpty()) {
            result.append(NULL_ARRAY);
            return;
        }
        result.append('*').append(streamEntries.size()).append(CRLF);
        for (StreamEntry entry : streamEntries) {
            result.append("*2\r\n");
            appendBulkString(result, entry.id().toString());
            appendFields(result, entry.fields());
        }
    }

    private static void appendFields(StringBuilder result, Map<String, String> fields) {
        result.append('*').append(fields.size() * 2).append(CRLF);
        for (Map.Entry<String, String> field : fields.entrySet()) {
            appendBulkString(result, field.getKey());
            appendBulkString(result, field.getValue());
        }
    }

    private static void appendBulkString(StringBuilder result, String value) {
        if (value == null) {
            result.append(NULL_BULK);
            return;
        }
        result.append('$')
                .append(value.length())
                .append(CRLF)
                .append(value)
                .append(CRLF);
    }
}
