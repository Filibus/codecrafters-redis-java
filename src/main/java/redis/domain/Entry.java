package redis.domain;

public record Entry(String value, Long expiresAtMillis) {
    public boolean isExpired() {
        return expiresAtMillis != null && System.currentTimeMillis() >= expiresAtMillis;
    }
}
