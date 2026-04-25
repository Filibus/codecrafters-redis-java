package redis.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import redis.domain.Entry;

/**
 * In-memory list storage with FIFO BLPOP waiters.
 */
public final class ListStore {

    private final Map<String, List<Entry>> listData = new HashMap<>();
    private final Map<String, Deque<Long>> blPopWaiters = new HashMap<>();
    private long nextBlPopWaiterId = 0L;

    public List<Entry> addToList(String key, List<String> values) {
        List<Entry> listElements = listForUpdate(key);
        for (String v : values) {
            listElements.add(new Entry(v, null));
        }
        return listElements;
    }

    public List<Entry> prependToList(String key, List<String> values) {
        List<Entry> listElements = listForUpdate(key);
        for (String v : values) {
            listElements.addFirst(new Entry(v, null));
        }
        return listElements;
    }

    public Entry popElement(String key) {
        List<Entry> listElements = listData.get(key);
        if (listElements == null || listElements.isEmpty()) {
            return null;
        }
        return listElements.removeFirst();
    }

    public List<Entry> popElements(String key, int count) {
        List<Entry> listElements = listData.get(key);
        if (listElements == null || listElements.isEmpty()) {
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

    /**
     * BLPOP with {@code blockMillis} (same as previous implementation: {@code 0} = block forever,
     * {@code &gt;0} = deadline in ms from now). The external {@code lock} must be the same object
     * used to guard all other store mutations in {@link DataStore}.
     *
     * @param timeoutMillis max wait: {@code 0} means block forever, {@code &gt;0} until deadline
     */
    public Entry bLPop(Object lock, String key, long timeoutMillis) {
        var deadline = System.currentTimeMillis() + timeoutMillis;
        long waiterId;
        synchronized (lock) {
            waiterId = ++nextBlPopWaiterId;
            blPopWaiters.computeIfAbsent(key, ignored -> new LinkedList<>()).addLast(waiterId);
        }

        try {
            while (timeoutMillis == 0L || System.currentTimeMillis() < deadline) {
                synchronized (lock) {
                    List<Entry> listElements = listForUpdate(key);
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
            synchronized (lock) {
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

    public int getListSize(String key) {
        List<Entry> listElements = listData.get(key);
        return listElements == null ? 0 : listElements.size();
    }

    public List<String> lRange(String key, int start, int stop) {
        List<Entry> items = listData.get(key);
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

    public void clear(String key) {
        listData.remove(key);
    }

    private List<Entry> listForUpdate(String key) {
        return listData.computeIfAbsent(key, k -> new ArrayList<>());
    }
}
