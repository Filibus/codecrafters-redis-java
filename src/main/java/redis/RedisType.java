package redis;

enum RedisType {
    STRING("string"),
    LIST("list"),
    STREAM("stream"),
    NONE("none");

    private final String wireValue;

    RedisType(String wireValue) {
        this.wireValue = wireValue;
    }

    String wireValue() {
        return wireValue;
    }
}