package redis.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import redis.domain.StreamEntry;
import redis.domain.StreamId;

public final class StreamStore {

    private final Map<String, List<StreamEntry>> streamData = new HashMap<>();

    public StreamId xAdd(String key, String id, List<String> keyValuePairs) {
        List<StreamEntry> streamEntries = streamData.computeIfAbsent(key, k -> new ArrayList<>());
        StreamId newStreamId = generateStreamId(streamEntries, StreamId.from(id));
        if (newStreamId.compareTo(StreamId.from("0-0")) == 0) {
            throw new IllegalArgumentException("The ID specified in XADD must be greater than 0-0");
        }
        if (streamEntries.isEmpty()) {
            streamEntries.add(createStreamEntry(newStreamId, keyValuePairs));
        } else {
            StreamEntry lastEntry = streamEntries.getLast();
            if (lastEntry.id().compareTo(newStreamId) >= 0) {
                throw new IllegalArgumentException(
                        "The ID specified in XADD is equal or smaller than the target stream top item");
            } else {
                streamEntries.add(createStreamEntry(newStreamId, keyValuePairs));
            }
        }
        return streamEntries.getLast().id();
    }

    public List<StreamEntry> rangeStreamEntries(String key, String startId, String endId) {
        List<StreamEntry> entries = streamData.computeIfAbsent(key, k -> new ArrayList<>());
        StreamId start = StreamId.fromRange(startId);
        StreamId end = StreamId.fromRange(endId);
        return entries.stream()
                .filter(entry -> entry.id().compareTo(start) >= 0
                        && entry.id().compareTo(end) <= 0)
                .toList();
    }

    /**
     * Returns entries strictly after {@code startId} (XREAD semantics).
     */
    public List<StreamEntry> xRead(String key, String startId) {
        List<StreamEntry> entries = streamData.get(key);
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyList();
        }
        StreamId start = StreamId.fromRange(startId);
        return entries.stream()
                .filter(entry -> entry.id().compareTo(start) > 0)
                .toList();
    }

    /**
     * Returns the id of the latest entry, or {@code 0-0} when the stream is empty or missing.
     */
    public StreamId lastStreamId(String key) {
        List<StreamEntry> entries = streamData.get(key);
        if (entries == null || entries.isEmpty()) {
            return new StreamId(0L, 0L);
        }
        return entries.getLast().id();
    }

    public Map<String, List<StreamEntry>> xReadBlocking(
            Object lock,
            LinkedHashMap<String, StreamId> startAfterIds, Long timeoutMillis) {

        long deadline = timeoutMillis == null || timeoutMillis == 0L
                ? Long.MAX_VALUE
                : System.currentTimeMillis() + timeoutMillis;
        boolean waitForever = timeoutMillis != null && timeoutMillis == 0L;

        while (true) {
            LinkedHashMap<String, List<StreamEntry>> result;
            synchronized (lock) {
                result = collectNewEntries(startAfterIds);
            }
            if (!result.isEmpty()) {
                return result;
            }
            if (timeoutMillis == null) {
                return null;
            }
            if (!waitForever && System.currentTimeMillis() >= deadline) {
                return null;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
    }

    private LinkedHashMap<String, List<StreamEntry>> collectNewEntries(
            LinkedHashMap<String, StreamId> startAfterIds) {
        LinkedHashMap<String, List<StreamEntry>> result = new LinkedHashMap<>();
        for (Map.Entry<String, StreamId> e : startAfterIds.entrySet()) {
            List<StreamEntry> entries = streamData.get(e.getKey());
            if (entries == null || entries.isEmpty()) {
                continue;
            }
            StreamId after = e.getValue();
            List<StreamEntry> matched = entries.stream()
                    .filter(entry -> entry.id().compareTo(after) > 0)
                    .toList();
            if (!matched.isEmpty()) {
                result.put(e.getKey(), matched);
            }
        }
        return result;
    }

    private static StreamEntry createStreamEntry(StreamId newStreamId, List<String> keyValuePairs) {
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValuePairs.size(); i += 2) {
            fields.put(keyValuePairs.get(i), keyValuePairs.get(i + 1));
        }
        return new StreamEntry(newStreamId, fields);
    }

    private static StreamId generateStreamId(List<StreamEntry> streamEntries, StreamId streamId) {
        if (streamId.milliSeconds() == null) {
            Long timeStamp = System.currentTimeMillis();
            return streamEntries.stream()
                    .filter(entry -> entry.id().milliSeconds().equals(timeStamp)).findFirst()
                    .map(entry -> {
                        var seqIncremented = entry.id().sequenceNumber() + 1L;
                        return new StreamId(timeStamp, seqIncremented);
                    }).orElseGet(() -> new StreamId(timeStamp, 0L));
        }
        if (streamId.sequenceNumber() == null) {
            var sequenceNumber = generateSequenceNumber(streamEntries, streamId.milliSeconds());
            return new StreamId(streamId.milliSeconds(), sequenceNumber);
        }
        return streamId;
    }

    private static Long generateSequenceNumber(List<StreamEntry> streamEntries, Long milliSeconds) {
        if (milliSeconds == 0L) {
            return 1L;
        }
        var seqNumber = streamEntries.stream()
                .filter(se -> se.id().milliSeconds().equals(milliSeconds))
                .max(Comparator.comparing(se -> se.id().sequenceNumber()))
                .map(StreamEntry::id).map(StreamId::sequenceNumber).orElse(null);
        if (seqNumber == null) {
            return 0L;
        }
        return ++seqNumber;
    }

    public void clear(String key) {
        streamData.remove(key);
    }
}
