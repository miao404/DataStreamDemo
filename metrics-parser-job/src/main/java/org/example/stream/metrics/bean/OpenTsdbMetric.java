package org.example.stream.metrics.bean;

import java.util.Map;

public class OpenTsdbMetric {
    private String metric;
    private Map<String, String> tags;
    private long timestamp;
    private Object value;

    public OpenTsdbMetric() {
    }

    public String getMetric() {
        return this.metric;
    }

    public Map<String, String> getTags() {
        return this.tags;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public Object getValue() {
        return this.value;
    }

    public void setTimestamp(long ts) {
        this.timestamp = ts;
    }

    public void setTags(Map<String, String> newTags) {
        this.tags = newTags;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
