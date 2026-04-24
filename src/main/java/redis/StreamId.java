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

    public static StreamId fromRange(String id) {
        if("+".equalsIgnoreCase(id)){
            return new StreamId(Long.MAX_VALUE, Long.MAX_VALUE);
        } else if("-".equalsIgnoreCase(id)){
            return new StreamId(Long.MIN_VALUE, Long.MAX_VALUE);
        }
        String[] parts = id.split("-");
        if( parts.length == 0){
            return new StreamId(0L, 0L);
        }
        if( parts.length == 1){
            return new StreamId(Long.parseLong(parts[0]), 0L);
        }
        return new StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
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