package redis;

public class RespDataHolder<D> {
    RespDataType dataType;
    D data;

    public RespDataType getDataType() {
        return dataType;
    }

    public RespDataHolder( RespDataType dataType, D data) {
        this.data = data;
        this.dataType = dataType;
    }

    public D getData() {
        return data;
    }

    public void setDataType(RespDataType dataType) {
        this.dataType = dataType;
    }

    public void setData(D data) {
        this.data = data;
    }
}
