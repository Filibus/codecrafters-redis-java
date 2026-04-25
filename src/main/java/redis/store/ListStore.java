package redis.store;

import redis.domain.Entry;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe in-memory list store with proper BLPOP support.
 */
public final class ListStore {

    private final ReentrantLock lock = new ReentrantLock(true); // fair lock

    private final Map<String, Deque<Entry>> listData = new HashMap<>();
    private final Map<String, Condition> notEmptyConditions = new HashMap<>();

    // ---------------------------
    // LIST OPERATIONS
    // ---------------------------

    public List<Entry> addToList(String key, List<String> values) {
        lock.lock();
        try {
            Deque<Entry> list = listForUpdate(key);

            for (String v : values) {
                list.addLast(new Entry(v, null));
            }

            signalIfNeeded(key);

            return new ArrayList<>(list);
        } finally {
            lock.unlock();
        }
    }

    public List<Entry> prependToList(String key, List<String> values) {
        lock.lock();
        try {
            Deque<Entry> list = listForUpdate(key);

            for (String v : values) {
                list.addFirst(new Entry(v, null));
            }

            signalIfNeeded(key);

            return new ArrayList<>(list);
        } finally {
            lock.unlock();
        }
    }

    public Entry popElement(String key) {
        lock.lock();
        try {
            Deque<Entry> list = listData.get(key);
            if (list == null || list.isEmpty()) {
                return null;
            }
            return list.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    public List<Entry> popElements(String key, int count) {
        lock.lock();
        try {
            Deque<Entry> list = listData.get(key);
            if (list == null || list.isEmpty()) {
                return Collections.emptyList();
            }

            List<Entry> result = new ArrayList<>();
            while (count-- > 0 && !list.isEmpty()) {
                result.add(list.pollFirst());
            }

            return result;
        } finally {
            lock.unlock();
        }
    }

    // ---------------------------
    // BLPOP (CORE FIX)
    // ---------------------------

    public Entry bLPop(String key, long timeoutMillis) {
        lock.lock();
        try {
            Condition condition = notEmptyConditions
                    .computeIfAbsent(key, k -> lock.newCondition());

            long nanos = timeoutMillis > 0
                    ? TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
                    : 0;

            while (true) {
                Deque<Entry> list = listData.get(key);

                if (list != null && !list.isEmpty()) {
                    return list.pollFirst();
                }

                if (timeoutMillis == 0) {
                    condition.await();
                } else {
                    if (nanos <= 0) return null;
                    nanos = condition.awaitNanos(nanos);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    // ---------------------------
    // RANGE
    // ---------------------------

    public List<String> lRange(String key, int start, int stop) {
        lock.lock();
        try {
            Deque<Entry> list = listData.get(key);
            if (list == null || list.isEmpty()) {
                return Collections.emptyList();
            }

            List<Entry> items = new ArrayList<>(list);

            int n = items.size();
            int s = start < 0 ? Math.max(0, n + start) : start;
            int t = stop < 0 ? Math.max(0, n + stop) : Math.min(stop, n - 1);

            if (s >= n || s > t) {
                return Collections.emptyList();
            }

            return items.subList(s, t + 1)
                    .stream()
                    .map(Entry::value)
                    .toList();

        } finally {
            lock.unlock();
        }
    }

    // ---------------------------
    // SIZE / CLEAR
    // ---------------------------

    public int getListSize(String key) {
        lock.lock();
        try {
            Deque<Entry> list = listData.get(key);
            return list == null ? 0 : list.size();
        } finally {
            lock.unlock();
        }
    }

    public void clear(String key) {
        lock.lock();
        try {
            listData.remove(key);
            notEmptyConditions.remove(key);
        } finally {
            lock.unlock();
        }
    }

    // ---------------------------
    // INTERNAL HELPERS
    // ---------------------------

    private Deque<Entry> listForUpdate(String key) {
        return listData.computeIfAbsent(key, k -> new LinkedList<>());
    }

    private void signalIfNeeded(String key) {
        Condition condition = notEmptyConditions.get(key);
        if (condition != null) {
            condition.signal(); // wakes oldest BLPOP waiter
        }
    }
}