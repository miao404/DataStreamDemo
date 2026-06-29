package com.meituan.data.rt.metrics.utils.extractor;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.meituan.data.rt.metrics.bean.PetraMetric;
import com.meituan.data.rt.metrics.config.JobConfig;
import com.meituan.data.rt.metrics.config.MetricsToPetraConfig;
import com.meituan.data.rt.metrics.utils.GsonUtil;
import com.meituan.data.rt.metrics.utils.extractor.flink.FlinkHistogramMetricsUtil;
import com.meituan.data.rt.metrics.utils.extractor.flink.FlinkMetricsUtil;
import com.meituan.data.rt.metrics.utils.extractor.storm.StormMapAvgMetricsUtil;
import com.meituan.data.rt.metrics.utils.extractor.storm.StormMapGcMetricsUtil;
import com.meituan.data.rt.metrics.utils.extractor.storm.StormMapKafkaOffsetMetricsUtil;
import com.meituan.data.rt.metrics.utils.extractor.storm.StormMapMemoryMetricsUtil;
import com.meituan.data.rt.metrics.utils.extractor.storm.StormMapSumMetricsUtil;
import com.meituan.data.rt.metrics.utils.extractor.storm.StormSingleValueMetricsUtil;
import com.meituan.data.rt.metrics.utils.extractor.storm.StormUserDefinedMetricsUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsExtractor {
    private static final Logger logger = LoggerFactory.getLogger(MetricsExtractor.class);
    private JsonObject json;
    private Map<String, String> commonTags;
    private Map<String, Map<String, String>> extraTags;
    private Long timestamp;
    private List<PetraMetric> metrics;

    public MetricsExtractor() {
    }

    public MetricsExtractor(String json) {
        try {
            this.json = GsonUtil.toJsonObject(json);
        } catch (Exception e) {
            logger.error("Metrics not a valid JSON string: " + json + "\nwith Exception: ", e);
            this.json = null;
        }

    }

    public void init(JobConfig config) {
        this.setTimestamp(this.json.get("timestamp").getAsLong());
        String engineType = this.json.get("engine_type").getAsString();
        List<String> tagNames = ((MetricsToPetraConfig)config).getOpentsdbTags();
        Map<String, String> tags = new HashMap();

        for(String tagName : tagNames) {
            try {
                if (this.json.get(tagName) != null) {
                    String tagValue = this.json.get(tagName).getAsString();
                    if (tagValue != null && tagValue.length() >= 0) {
                        tags.put(tagName, tagValue);
                    }
                }
            } catch (Exception e) {
                logger.warn("Tags extract exception: " + this.json, e);
            }
        }

        this.setCommonTags(tags);
        Map<String, List<String>> extraTagsMap = ((MetricsToPetraConfig)config).getExtraTagsMap();
        Map<String, Map<String, String>> extraTags = new HashMap();

        for(Map.Entry<String, List<String>> entry : extraTagsMap.entrySet()) {
            Map<String, String> newExtraTags = new HashMap();

            for(String tagName : entry.getValue()) {
                if (this.json.has(tagName)) {
                    String tagValue = this.json.get(tagName).getAsString();
                    if (tagValue != null && tagValue.length() >= 0) {
                        newExtraTags.put(tagName, tagValue);
                    }
                }
            }

            extraTags.put(entry.getKey(), newExtraTags);
        }

        this.setExtraTags(extraTags);
        this.setMetrics(new ArrayList());
        JsonArray metricsArray = this.json.getAsJsonArray("metrics");
        Map<String, List<String>> extractorType = ((MetricsToPetraConfig)config).getExtractorType();

        for(JsonElement metricJson : metricsArray) {
            String name = metricJson.getAsJsonObject().get("name").getAsString();
            MetricsUtil metricsUtil = null;
            Map<String, String> customTags = new HashMap();
            if (engineType.equals("storm")) {
                if (((List)extractorType.get("storm_map_sum")).contains(name)) {
                    metricsUtil = new StormMapSumMetricsUtil();
                } else if (((List)extractorType.get("storm_map_avg")).contains(name)) {
                    metricsUtil = new StormMapAvgMetricsUtil();
                } else if (((List)extractorType.get("storm_single_value")).contains(name)) {
                    metricsUtil = new StormSingleValueMetricsUtil();
                    customTags.put("tm_id", this.json.get("tm_id").getAsString());
                } else if (((List)extractorType.get("storm_map_memory")).contains(name)) {
                    metricsUtil = new StormMapMemoryMetricsUtil();
                    customTags.put("tm_id", this.json.get("tm_id").getAsString());
                } else if (((List)extractorType.get("storm_map_gc")).contains(name)) {
                    metricsUtil = new StormMapGcMetricsUtil();
                    customTags.put("tm_id", this.json.get("tm_id").getAsString());
                } else if (((List)extractorType.get("storm_map_kafka_offset")).contains(name)) {
                    metricsUtil = new StormMapKafkaOffsetMetricsUtil();
                } else if (GsonUtil.toJson(metricJson.getAsJsonObject().get("value")).contains("\"metricType\"")) {
                    metricsUtil = new StormUserDefinedMetricsUtil();
                }
            } else if (engineType.equals("flink")) {
                if (name.endsWith("kafkaOffset")) {
                    metricsUtil = new StormMapKafkaOffsetMetricsUtil();
                } else if (name.startsWith("H")) {
                    metricsUtil = new FlinkHistogramMetricsUtil();
                } else {
                    metricsUtil = new FlinkMetricsUtil();
                }
            }

            Set<String> multiMetricsSet = ((MetricsToPetraConfig)config).getMultiMetricsSet();
            if (metricsUtil != null) {
                metricsUtil.setFullName(name);
                metricsUtil.setValue(metricJson.getAsJsonObject().get("value"));
                String delimiter = "_";
                if (this.json.get("delimiter") != null && this.json.get("delimiter").getAsString().length() != 0) {
                    delimiter = this.json.get("delimiter").getAsString();
                }

                metricsUtil.init(config, delimiter);
                if (metricsUtil.getValid() == Boolean.TRUE) {
                    if (multiMetricsSet.contains(metricsUtil.getType())) {
                        List<MetricsUtil.MetricsBean> metricsBeans = (List<MetricsUtil.MetricsBean>) metricsUtil.getValue();
                        for(MetricsUtil.MetricsBean metric : metricsBeans) {
                            PetraMetric petraMetric = new PetraMetric();
                            petraMetric.setTs(this.getTimestamp());
                            Map<String, String> allTags = new HashMap();
                            allTags.putAll(metricsUtil.getTags());
                            allTags.putAll(metric.getTags());
                            allTags.putAll(this.commonTags);
                            if (extraTags.containsKey(metric.getName())) {
                                allTags.putAll((Map)extraTags.get(metric.getName()));
                            }

                            allTags.putAll(customTags);
                            petraMetric.setTags(allTags);
                            Map<String, List<Object>> kvs = new HashMap();
                            kvs.put(metric.getName(), Arrays.asList(metric.getValue()));
                            petraMetric.setKvs(kvs);
                            this.getMetrics().add(petraMetric);
                        }
                    } else {
                        PetraMetric petraMetric = new PetraMetric();
                        petraMetric.setTs(this.getTimestamp());
                        Map<String, String> allTags = metricsUtil.getTags();
                        allTags.putAll(this.commonTags);
                        if (extraTags.containsKey(metricsUtil.getMetricsName())) {
                            allTags.putAll((Map)extraTags.get(metricsUtil.getMetricsName()));
                        }

                        allTags.putAll(customTags);
                        petraMetric.setTags(allTags);
                        Map<String, List<Object>> kvs = new HashMap();
                        kvs.put(metricsUtil.getMetricsName(), Arrays.asList(metricsUtil.getValue()));
                        petraMetric.setKvs(kvs);
                        this.getMetrics().add(petraMetric);
                    }
                }
            }
        }

    }

    public JsonObject getJson() {
        return this.json;
    }

    public void setJson(JsonObject json) {
        this.json = json;
    }

    public Map<String, String> getCommonTags() {
        return this.commonTags;
    }

    public void setCommonTags(Map<String, String> commonTags) {
        this.commonTags = commonTags;
    }

    public Map<String, Map<String, String>> getExtraTags() {
        return this.extraTags;
    }

    public void setExtraTags(Map<String, Map<String, String>> extraTags) {
        this.extraTags = extraTags;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public List<PetraMetric> getMetrics() {
        return this.metrics;
    }

    public void setMetrics(List<PetraMetric> metrics) {
        this.metrics = metrics;
    }
}
