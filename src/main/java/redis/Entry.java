package redis;

public record Entry(String value, Long expiresAtMillis) {
    boolean isExpired() {
        return expiresAtMillis != null && System.currentTimeMillis() >= expiresAtMillis;
    }
}
