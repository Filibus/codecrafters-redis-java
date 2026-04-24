package redis;

public record StreamId(Long milliSeconds, Long sequenceNumber) implements Comparable<StreamId> {
    public static StreamId from(String id) {
        String[] parts = id.split("-");
        return new StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
    }

    @Override
    public String toString() {
        return milliSeconds() + "-" + sequenceNumber();
    }

    @Override
    public int compareTo(StreamId o) {
        if (this.milliSeconds().equals(o.milliSeconds())) {
            return this.sequenceNumber().compareTo(o.sequenceNumber());
        }
        return this.milliSeconds().compareTo(o.milliSeconds());
    }
}