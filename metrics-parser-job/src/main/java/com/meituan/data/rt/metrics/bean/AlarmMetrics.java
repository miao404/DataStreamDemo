package com.meituan.data.rt.metrics.bean;

public class AlarmMetrics {
    private String metricName;
    private String stormName;
    private String type;
    private Long timestamp;
    private Double value;

    public AlarmMetrics() {
    }

    public AlarmMetrics(String metricName, String stormName, String type, Long timestamp, Double value) {
        this.metricName = metricName;
        this.stormName = stormName;
        this.type = type;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getMetricName() {
        return this.metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getStormName() {
        return this.stormName;
    }

    public void setStormName(String stormName) {
        this.stormName = stormName;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getValue() {
        return this.value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
