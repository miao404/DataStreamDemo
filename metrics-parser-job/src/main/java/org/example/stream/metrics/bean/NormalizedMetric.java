package org.example.stream.metrics.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NormalizedMetric implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(NormalizedMetric.class);
    private Map<String, List<Object>> kvs;
    private Map<String, String> tags;
    private Long ts;

    public String replaceForOpenTSDB(String string) {
        return string != null ? string.replaceAll("[^A-Za-z0-9_/.]+", "_") : string;
    }

    public NormalizedMetric() {
    }

    public NormalizedMetric(Map<String, List<Object>> kvs, Map<String, String> tags, Long ts) {
        this.kvs = kvs;
        this.tags = tags;
        this.ts = ts;
    }

    public Map<String, List<Object>> getKvs() {
        return this.kvs;
    }

    public void setKvs(Map<String, List<Object>> kvs) {
        Map<String, List<Object>> cleanedKvs = new HashMap();

        for(Map.Entry<String, List<Object>> entry : kvs.entrySet()) {
            cleanedKvs.put(this.replaceForOpenTSDB((String)entry.getKey()), entry.getValue());
        }

        this.kvs = cleanedKvs;
    }

    public Map<String, String> getTags() {
        return this.tags;
    }

    public void setTags(Map<String, String> newTags) {
        Map<String, String> cleanedTags = new HashMap();

        for(Map.Entry<String, String> entry : newTags.entrySet()) {
            cleanedTags.put(this.replaceForOpenTSDB((String)entry.getKey()), this.replaceForOpenTSDB((String)entry.getValue()));
        }

        this.tags = cleanedTags;
    }

    public Long getTs() {
        return this.ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}