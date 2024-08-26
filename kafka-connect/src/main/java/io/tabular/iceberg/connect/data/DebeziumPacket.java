package io.tabular.iceberg.connect.data;

import java.util.Map;

public class DebeziumPacket {

    private Map<String, Object> before;
    private Map<String, Object> after;
    private Map<String, Object> source;
    private String op;
    private String transaction;
    private long ts_ms;
    private long ts_us;
    private long ts_ns;

    public Map<String, Object> getBefore() {
        return before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setOp(String op) {
        this.op = op;
    }
}
