package redis;

public record StreamId(Long milliSeconds, Long sequenceNumber) implements Comparable<StreamId> {
    public static StreamId from(String id) {
        if("*".equalsIgnoreCase(id)){
            return new StreamId(null, null);
        }
        String[] parts = id.split("-");
        if( parts.length != 2){
            throw new IllegalArgumentException("Invalid ID format");
        }
        else if (parts[1].equalsIgnoreCase("*")) {
            return new StreamId(Long.parseLong(parts[0]), null);
        }
        Long sequenceNumber = Long.parseLong(parts[1]);
        return new StreamId(Long.parseLong(parts[0]), sequenceNumber);
    }

    @Override
    public String toString() {
        return milliSeconds() + "-" + (sequenceNumber() == null ? "*" : sequenceNumber());
    }

    @Override
    public int compareTo(StreamId o) {
        if (this.milliSeconds().equals(o.milliSeconds())) {
            return this.sequenceNumber().compareTo(o.sequenceNumber());
        }
        return this.milliSeconds().compareTo(o.milliSeconds());
    }
}