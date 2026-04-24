package redis;

public enum RespDataType {
    SIMPLE_STRING('+'),
    ERROR('-'),
    INTEGER(':'),
    BULK_STRING('$'),
    ARRAY('*'),
    NULL('_'),
    BOOLEAN('#'),
    UNKNOWN('?');

    private final char prefix;

    RespDataType(char prefix) {
        this.prefix = prefix;
    }

    public char getPrefix() {
        return prefix;
    }

    @Override
    public String toString() {
        return String.valueOf(prefix);
    }

    public static RespDataType fromPrefix(char prefix) {
        for (RespDataType type : values()) {
            if (type.prefix == prefix) {
                return type;
            }
        }
        return UNKNOWN;
    }

    public static RespDataType fromString(String str) {
        try {
            return valueOf(str.toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
