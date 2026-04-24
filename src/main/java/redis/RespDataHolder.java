package redis;

public record RespDataHolder<D>(RespDataType dataType, D data) {
}
