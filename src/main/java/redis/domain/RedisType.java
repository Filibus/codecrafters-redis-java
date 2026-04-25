package redis.domain;

public enum RedisType {
    STRING("string"),
    LIST("list"),
    STREAM("stream"),
    NONE("none");

    private final String wireValue;

    RedisType(String wireValue) {
        this.wireValue = wireValue;
    }

    public String wireValue() {
        return wireValue;
    }
}
