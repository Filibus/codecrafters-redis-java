package redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class RedisInMemory {

    private final Map<String, List<Entry>> redisData = new HashMap<>();
    private final Map<String, Entry> redisDataSimple = new HashMap<>();
    private final Map<String, List<StreamEntry>> redisDataStream = new HashMap<>();
    private final Map<String, RedisType> keyTypes = new HashMap<>();
    private final Map<String, Deque<Long>> blPopWaiters = new HashMap<>();
    private long nextBlPopWaiterId = 0L;

    public List<Entry> addToList(String key, List<String> values) {
        clearNonListStorage(key);
        List<Entry> listElements = redisData.computeIfAbsent(key, k -> new ArrayList<>());
        for (String v : values) {
            listElements.add(new Entry(v, null));
        }
        keyTypes.put(key, RedisType.LIST);
        return listElements;
    }

    public List<Entry> prependToList(String key, List<String> values) {
        clearNonListStorage(key);
        List<Entry> listElements = redisData.computeIfAbsent(key, k -> new ArrayList<>());
        for (String v : values) {
            listElements.addFirst(new Entry(v, null));
        }
        keyTypes.put(key, RedisType.LIST);
        return listElements;
    }

    public Entry popEelement(String key) {
        List<Entry> listElements = redisData.computeIfAbsent(key, k -> new ArrayList<>());
        if (listElements.isEmpty()) {
            return null;
        }
        return listElements.removeFirst();
    }

    public List<Entry> popEelements(String key, Integer count) {
        List<Entry> listElements = redisData.computeIfAbsent(key, k -> new ArrayList<>());
        if (listElements.isEmpty()) {
            return null;
        }
        int removedCount = 0;
        var removedElements = new ArrayList<Entry>();
        while (removedCount < count && !listElements.isEmpty()) {
            removedElements.add(listElements.removeFirst());
            removedCount++;
        }
        return removedElements;
    }

    public Entry blPop(String key, Long timeoutSeconds) {
        var seconds = timeoutSeconds == null ? 0 : timeoutSeconds;
        var deadline = System.currentTimeMillis() + seconds;
        long waiterId;
        synchronized (this) {
            waiterId = ++nextBlPopWaiterId;
            blPopWaiters.computeIfAbsent(key, ignored -> new LinkedList<>()).addLast(waiterId);
        }

        try {
            while (seconds == 0 || System.currentTimeMillis() < deadline) {
                synchronized (this) {
                    List<Entry> listElements = redisData.computeIfAbsent(key, k -> new ArrayList<>());
                    Deque<Long> waiters = blPopWaiters.get(key);
                    boolean isHeadWaiter = waiters != null
                            && !waiters.isEmpty()
                            && waiters.peekFirst() == waiterId;

                    if (isHeadWaiter && !listElements.isEmpty()) {
                        waiters.removeFirst();
                        if (waiters.isEmpty()) {
                            blPopWaiters.remove(key);
                        }
                        return listElements.removeFirst();
                    }
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return null;
        } finally {
            synchronized (this) {
                Deque<Long> waiters = blPopWaiters.get(key);
                if (waiters != null) {
                    waiters.remove(waiterId);
                    if (waiters.isEmpty()) {
                        blPopWaiters.remove(key);
                    }
                }
            }
        }
    }

    public Integer getListSize(String key) {
        List<Entry> listElements = redisData.computeIfAbsent(key, k -> new ArrayList<>());
        return listElements.size();
    }

    public List<String> lRange(String key, int start, int stop) {
        List<Entry> items = redisData.get(key);
        if (items == null || items.isEmpty()) {
            return Collections.emptyList();
        }
        int n = items.size();
        int s = start < 0 ? Math.max(0, n + start) : start;
        int t = stop < 0 ? Math.max(0, n + stop) : Math.min(stop, n - 1);
        if (s >= n || s > t) {
            return Collections.emptyList();
        }
        return items.subList(s, t + 1).stream().map(Entry::value).toList();
    }

    /**
     * Sets a value that expires after {@code ttlMillis} millieconds.
     */
    public void set(String key, String value, Long ttlMillis) {
        clearNonStringStorage(key);
        Long expiresAt = ttlMillis == null || ttlMillis <= 0 ?
                null : System.currentTimeMillis() + ttlMillis;
        redisDataSimple.put(key, new Entry(value, expiresAt));
        keyTypes.put(key, RedisType.STRING);
    }

    public String getType(String key) {
        RedisType type = keyTypes.getOrDefault(key, RedisType.NONE);
        return type.wireValue();
    }

    public StreamId xAdd(String key, String id, List<String> keyValuePairs) {
        List<StreamEntry> streamEntries = redisDataStream.computeIfAbsent(key, k -> new ArrayList<>());
        StreamId newStreamId = generateStreamId(streamEntries, StreamId.from(id));
        if (newStreamId.compareTo(StreamId.from("0-0")) == 0) {
            throw new IllegalArgumentException("The ID specified in XADD must be greater than 0-0");
        }
        if (streamEntries.isEmpty()) {
            streamEntries.add(createStreamEntry(newStreamId, keyValuePairs));
        } else {
            StreamEntry lastEntry = streamEntries.getLast();
            if (lastEntry.id().compareTo(newStreamId) >= 0) {
                throw new IllegalArgumentException("The ID specified in XADD is equal or smaller than the target stream top item");
            } else {
                streamEntries.add(createStreamEntry(newStreamId, keyValuePairs));
            }
        }
        clearNonStreamStorage(key);
        keyTypes.put(key, RedisType.STREAM);
        return streamEntries.getLast().id();
    }

    public List<StreamEntry> rangeStreamEntries(String key,
                                                String startId, String endId) {

        List<StreamEntry> entries = redisDataStream.computeIfAbsent(key, k -> new ArrayList<>());
        StreamId start = StreamId.fromRange(startId);
        StreamId endTimeStamp = StreamId.fromRange(endId);
        return entries.stream()
                .filter(entry -> entry.id().compareTo(start) >= 0
                        && entry.id().compareTo(endTimeStamp) <= 0)
                .toList();
    }

    private StreamEntry createStreamEntry(StreamId newStreamId, List<String> keyValuePairs) {
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValuePairs.size(); i += 2) {
            fields.put(keyValuePairs.get(i), keyValuePairs.get(i + 1));
        }
        return new StreamEntry(newStreamId, fields);
    }

    private StreamId generateStreamId(List<StreamEntry> streamEntries, StreamId streamId) {
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

    private Long generateSequenceNumber(List<StreamEntry> streamEntries, Long milliSeconds) {
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

    public Optional<Entry> getIfPresent(String key) {
        Entry e = redisDataSimple.get(key);
        if (e == null) {
            return Optional.empty();
        }
        if (e.isExpired()) {
            redisDataSimple.remove(key);
            keyTypes.remove(key);
            return Optional.empty();
        }
        return Optional.of(e);
    }

    private void clearNonStringStorage(String key) {
        redisData.remove(key);
        redisDataStream.remove(key);
    }

    private void clearNonListStorage(String key) {
        redisDataSimple.remove(key);
        redisDataStream.remove(key);
    }

    private void clearNonStreamStorage(String key) {
        redisDataSimple.remove(key);
        redisData.remove(key);
    }

}
