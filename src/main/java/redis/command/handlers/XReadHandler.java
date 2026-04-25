package redis.command.handlers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import redis.command.CommandHandler;
import redis.domain.StreamEntry;
import redis.domain.StreamId;
import redis.protocol.RespWriter;
import redis.store.DataStore;

public final class XReadHandler implements CommandHandler {

    private final DataStore store;

    public XReadHandler(DataStore store) {
        this.store = store;
    }

    @Override
    public String execute(List<String> args, String connectionId) {
        if (args.isEmpty()) {
            return syntaxError();
        }

        Long blockMillis = null;
        int cursor = 0;
        if (args.getFirst().equalsIgnoreCase("BLOCK")) {
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

    private LinkedHashMap<String, StreamId> resolveStartIds(
            List<String> streamKeys, List<String> streamIds) {
        LinkedHashMap<String, StreamId> startAfterIds = new LinkedHashMap<>();
        for (int i = 0; i < streamKeys.size(); i++) {
            String key = streamKeys.get(i);
            String rawId = streamIds.get(i);
            StreamId start = "$".equals(rawId)
                    ? store.lastStreamId(key)
                    : StreamId.fromRange(rawId);
            startAfterIds.put(key, start);
        }
        return startAfterIds;
    }

    private String readStreamsNonBlocking(LinkedHashMap<String, StreamId> startAfterIds) {
        LinkedHashMap<String, List<StreamEntry>> streamEntries = new LinkedHashMap<>();
        for (Map.Entry<String, StreamId> e : startAfterIds.entrySet()) {
            List<StreamEntry> entries = store.xRead(e.getKey(), e.getValue().toString());
            if (!entries.isEmpty()) {
                streamEntries.put(e.getKey(), entries);
            }
        }
        if (streamEntries.isEmpty()) {
            return RespWriter.NULL_ARRAY;
        }
        return RespWriter.xreadResult(streamEntries);
    }

    private String readStreamsBlocking(LinkedHashMap<String, StreamId> startAfterIds, long blockMillis) {
        Map<String, List<StreamEntry>> result = store.xReadBlocking(startAfterIds, blockMillis);
        if (result == null || result.isEmpty()) {
            return RespWriter.NULL_ARRAY;
        }
        return RespWriter.xreadResult(result);
    }

    private static String syntaxError() {
        return RespWriter.error("syntax error");
    }
}
