package org.example.stream.metrics.utils;

import org.example.stream.metrics.bean.OpenTsdbMetric;
import org.example.stream.metrics.bean.NormalizedMetric;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricsFormatterUtil {
    private static final int MAX_TAG_VALUE_LENGTH = 256;

    public MetricsFormatterUtil() {
    }

    public static List<OpenTsdbMetric> normalizedToOpenTsdb(NormalizedMetric normalizedMetric) {
        List<OpenTsdbMetric> openTsdbMetrics = new ArrayList();
        Map<String, String> newTags = truncateTagV(normalizedMetric.getTags());

        for(Map.Entry<String, List<Object>> metric : normalizedMetric.getKvs().entrySet()) {
            for(Object value : (List)metric.getValue()) {
                OpenTsdbMetric openTsdbMetric = new OpenTsdbMetric();
                openTsdbMetric.setTimestamp(normalizedMetric.getTs());
                openTsdbMetric.setTags(newTags);
                openTsdbMetric.setMetric((String)metric.getKey());
                openTsdbMetric.setValue(value);
                openTsdbMetrics.add(openTsdbMetric);
            }
        }

        return openTsdbMetrics;
    }

    public static List<String> normalizedToJsonString(NormalizedMetric normalizedMetric) {
        List<String> jsonMetrics = new ArrayList();
        Map<String, String> newTags = truncateTagV(normalizedMetric.getTags());

        for(Map.Entry<String, List<Object>> metric : normalizedMetric.getKvs().entrySet()) {
            Map<String, String> metricEntry = new HashMap();

            for(Object value : (List)metric.getValue()) {
                metricEntry.putAll(newTags);
                metricEntry.put("timestamp", normalizedMetric.getTs().toString());
                metricEntry.put("metric", metric.getKey());
                metricEntry.put("value", value.toString());
                jsonMetrics.add(GsonUtil.toJson(metricEntry));
            }
        }

        return jsonMetrics;
    }

    public static String openTsdbToJsonString(OpenTsdbMetric openTsdbMetric) {
        Map<String, String> metric = new HashMap(openTsdbMetric.getTags());
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(openTsdbMetric.getTimestamp() * 1000L), ZoneId.systemDefault());
        metric.put("metric_datetime", df.format(localDateTime));
        metric.put("metric", openTsdbMetric.getMetric());
        if (openTsdbMetric.getValue().toString().equals("NaN")) {
            metric.put("value", "0.0");
        } else {
            metric.put("value", openTsdbMetric.getValue().toString());
        }

        return GsonUtil.toJson(metric);
    }

    private static Map<String, String> truncateTagV(Map<String, String> tags) {
        Map<String, String> newTags = new HashMap();

        for(Map.Entry<String, String> entry : tags.entrySet()) {
            String key = (String)entry.getKey();
            String value = (String)entry.getValue();
            if (value != null && value.length() > 256) {
                value = value.substring(0, 256);
            }

            newTags.put(key, value);
        }

        return newTags;
    }
}
