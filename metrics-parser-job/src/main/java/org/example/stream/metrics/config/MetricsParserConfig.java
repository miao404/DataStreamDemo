package org.example.stream.metrics.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsParserConfig extends JobConfig {
    private static final Logger logger = LoggerFactory.getLogger(MetricsParserConfig.class);
    private String metricsNameDelimiter;
    private List<String> opentsdbTags;
    private List<String> rocksdbExtraTags;
    private Map<String, List<String>> scopeTagsMap;
    private Map<String, List<String>> extraTagsMap;
    private Map<String, List<String>> extractorType;
    private Set<String> multiMetricsSet;
    private Set<String> displayedMetricsSet;
    private Boolean displayUserDefinedMetrics;
    private Boolean sinkAllMetrics;
    private Boolean outputNormalizedMetrics;

    public MetricsParserConfig() {
        InputStream inputStream = MetricsParserConfig.class.getClassLoader().getResourceAsStream("config.properties");

        try {
            Configuration config = ParameterTool.fromPropertiesFile(inputStream).getConfiguration();
            this.initFromConfiguration(config);
        } catch (IOException e) {
            logger.error("Unable to load default config from config.properties!", e);
        }

    }

    public MetricsParserConfig(Configuration config) {
        this.initFromConfiguration(config);
    }

    public void initFromConfiguration(Configuration config) {
        super.initFromConfiguration(config);
        if (config.containsKey("rt.metrics.output.name.delimiter")) {
            this.setMetricsNameDelimiter(config.getString("rt.metrics.output.name.delimiter", "."));
        }

        if (config.containsKey("rt.metrics.tags")) {
            this.setOpentsdbTags(config.getString("rt.metrics.tags", "[job_name]"));
        }

        if (config.containsKey("rt.metrics.rocksdb.extra.tags")) {
            this.setRocksdbExtraTags(config.getString("rt.metrics.rocksdb.extra.tags", "[column_family]"));
        }

        if (config.containsKey("rt.metrics.scope.tags.map")) {
            this.setScopeTagsMap(config.getString("rt.metrics.scope.tags.map", "{JM=[scope];JM_JOB=[scope];TM=[scope];TM_JOB=[scope];TASK=[scope,task_name,subtask_index];OPERATOR=[scope,operator_id,operator_name,subtask_index]}"));
        }

        if (config.containsKey("rt.metrics.extra.tags.map")) {
            this.setExtraTagsMap(config.getString("rt.metrics.extra.tags.map", "{__ack-count=[host_name]}"));
        }

        if (config.containsKey("rt.metrics.extractor.type")) {
            this.setExtractorType(config.getString("rt.metrics.extractor.type", "{flink=[uptime,fullRestarts,numRecordsInPerSecond,numRecordsOutPerSecond,Status.JVM.CPU.Load,Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.NonHeap.Used,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.NonHeap.Used,Status.JVM.GarbageCollector.PS_Scavenge.Count,Status.JVM.GarbageCollector.PS_MarkSweep.Count,Status.Network.AvailableMemorySegments];flink_map_kafka_offset=[KafkaConsumer.current-offsets];storm_single_value=[uptimeSecs];storm_map_sum=[__ack-count,__fail-count,__execute-count];storm_map_avg=[__complete-latency,__execute-latency];storm_map_memory=[memory/nonHeap,memory/heap];storm_map_gc=[GC/ConcurrentMarkSweep,GC/ParNew];storm_map_kafka_offset=[org.example.stream.kafkaOffset,kafkaOffset]}"));
        }

        if (config.containsKey("rt.metrics.extractor.multi")) {
            this.setMultiMetricsSet(config.getString("rt.metrics.extractor.multi", "[storm_map_kafka_offset,flink_map_kafka_offset,storm_map_gc,storm_map_kafka_offset]"));
        }

        if (config.containsKey("rt.metrics.filter.display")) {
            this.setDisplayedMetricsSet(config.getString("rt.metrics.filter.display", "[uptime,fullRestarts,numRecordsInPerSecond,numRecordsOutPerSecond,Status.JVM.CPU.Load,Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.NonHeap.Used,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.NonHeap.Used,Status.JVM.GarbageCollector.PS_Scavenge.Count,Status.JVM.GarbageCollector.PS_MarkSweep.Count,Status.Network.AvailableMemorySegments]"));
        }

        if (config.containsKey("rt.metrics.filter.user-defined")) {
            this.setDisplayUserDefinedMetrics(config.getBoolean("rt.metrics.filter.user-defined", true));
        }

        if (config.containsKey("rt.metrics.output.all")) {
            this.setSinkAllMetrics(config.getBoolean("rt.metrics.output.all", false));
        }

        if (config.containsKey("rt.metrics.output.normalized")) {
            this.setOutputNormalizedMetrics(config.getBoolean("rt.metrics.output.normalized", false));
        }

    }

    public Configuration toConfiguration() {
        Configuration config = super.toConfiguration();
        config.setString("rt.metrics.output.name.delimiter", this.getMetricsNameDelimiter());
        config.setString("rt.metrics.tags", this.getOpentsdbTags().toString());
        config.setString("rt.metrics.rocksdb.extra.tags", this.getRocksdbExtraTags().toString());
        config.setString("rt.metrics.scope.tags.map", this.getScopeTagsMapString());
        config.setString("rt.metrics.extra.tags.map", this.getExtraTagsMapString());
        config.setString("rt.metrics.extractor.type", this.getExtractorTypeString());
        config.setString("rt.metrics.extractor.multi", this.getMultiMetricsSet().toString());
        config.setString("rt.metrics.filter.display", this.getDisplayedMetricsSet().toString());
        config.setString("rt.metrics.filter.user-defined", this.getDisplayUserDefinedMetrics().toString());
        config.setString("rt.metrics.output.all", this.getSinkAllMetrics().toString());
        config.setString("rt.metrics.output.normalized", this.getOutputNormalizedMetrics().toString());
        return config;
    }

    public String getMetricsNameDelimiter() {
        return this.metricsNameDelimiter;
    }

    public void setMetricsNameDelimiter(String metricsNameDelimiter) {
        this.metricsNameDelimiter = metricsNameDelimiter;
    }

    public List<String> getOpentsdbTags() {
        return this.opentsdbTags;
    }

    public void setOpentsdbTags(List<String> opentsdbTags) {
        this.opentsdbTags = opentsdbTags;
    }

    public void setOpentsdbTags(String opentsdbTags) {
        this.opentsdbTags = Arrays.asList(opentsdbTags.replace("[", "").replace("]", "").split(",\\s*"));
    }

    public List<String> getRocksdbExtraTags() {
        return this.rocksdbExtraTags;
    }

    public void setRocksdbExtraTags(List<String> rocksdbExtraTags) {
        this.rocksdbExtraTags = rocksdbExtraTags;
    }

    public void setRocksdbExtraTags(String rocksdbExtraTags) {
        this.rocksdbExtraTags = Arrays.asList(rocksdbExtraTags.replace("[", "").replace("]", "").split(",\\s*"));
    }

    public Map<String, List<String>> getScopeTagsMap() {
        return this.scopeTagsMap;
    }

    public String getScopeTagsMapString() {
        StringBuilder scopeTagsMapString = new StringBuilder("{");

        for(Map.Entry<String, List<String>> entry : this.getScopeTagsMap().entrySet()) {
            scopeTagsMapString.append(entry.toString());
            scopeTagsMapString.append(";");
        }

        scopeTagsMapString.append("}");
        return scopeTagsMapString.deleteCharAt(scopeTagsMapString.length() - 2).toString();
    }

    public void setScopeTagsMap(Map<String, List<String>> scopeTagsMap) {
        this.scopeTagsMap = scopeTagsMap;
    }

    public void setScopeTagsMap(String scopeTagsMapString) {
        this.scopeTagsMap = new HashMap();
        String[] pairs = scopeTagsMapString.replace("{", "").replace("}", "").split(";\\s*");

        for(String pair : pairs) {
            String[] keyValue = pair.split("=");
            this.scopeTagsMap.put(keyValue[0], Arrays.asList(keyValue[1].replace("[", "").replace("]", "").split(",\\s*")));
        }

    }

    public Map<String, List<String>> getExtraTagsMap() {
        return this.extraTagsMap;
    }

    public String getExtraTagsMapString() {
        StringBuilder extraTagsMapString = new StringBuilder("{");

        for(Map.Entry<String, List<String>> entry : this.getExtraTagsMap().entrySet()) {
            extraTagsMapString.append(entry.toString());
            extraTagsMapString.append(";");
        }

        extraTagsMapString.append("}");
        return extraTagsMapString.deleteCharAt(extraTagsMapString.length() - 2).toString();
    }

    public void setExtraTagsMap(Map<String, List<String>> extraTagsMap) {
        this.extraTagsMap = extraTagsMap;
    }

    public void setExtraTagsMap(String extraTagsMapString) {
        this.extraTagsMap = new HashMap();
        String[] pairs = extraTagsMapString.replace("{", "").replace("}", "").split(";\\s*");

        for(String pair : pairs) {
            String[] keyValue = pair.split("=");
            this.extraTagsMap.put(keyValue[0], Arrays.asList(keyValue[1].replace("[", "").replace("]", "").split(",\\s*")));
        }

    }

    public Map<String, List<String>> getExtractorType() {
        return this.extractorType;
    }

    public String getExtractorTypeString() {
        StringBuilder extractorTypeString = new StringBuilder("{");

        for(Map.Entry<String, List<String>> entry : this.getExtractorType().entrySet()) {
            extractorTypeString.append(entry.toString());
            extractorTypeString.append(";");
        }

        extractorTypeString.append("}");
        return extractorTypeString.deleteCharAt(extractorTypeString.length() - 2).toString();
    }

    public void setExtractorType(Map<String, List<String>> extractorType) {
        this.extractorType = extractorType;
    }

    public void setExtractorType(String extractorTypeString) {
        this.extractorType = new HashMap();
        String[] pairs = extractorTypeString.replace("{", "").replace("}", "").split(";\\s*");

        for(String pair : pairs) {
            String[] keyValue = pair.split("=");
            this.extractorType.put(keyValue[0], Arrays.asList(keyValue[1].replace("[", "").replace("]", "").split(",\\s*")));
        }

    }

    public Set<String> getMultiMetricsSet() {
        return this.multiMetricsSet;
    }

    public void setMultiMetricsSet(Set<String> multiMetricsSet) {
        this.multiMetricsSet = multiMetricsSet;
    }

    public void setMultiMetricsSet(String multiMetricsSetString) {
        this.multiMetricsSet = new HashSet();
        String[] multiMetrics = multiMetricsSetString.replace("[", "").replace("]", "").split(",\\s*");
        this.multiMetricsSet.addAll(Arrays.asList(multiMetrics));
    }

    public Set<String> getDisplayedMetricsSet() {
        return this.displayedMetricsSet;
    }

    public void setDisplayedMetricsSet(Set<String> displayedMetricsSet) {
        this.displayedMetricsSet = displayedMetricsSet;
    }

    public void setDisplayedMetricsSet(String displayedMetricsSetString) {
        this.displayedMetricsSet = new HashSet();
        String[] displayedMetrics = displayedMetricsSetString.replace("[", "").replace("]", "").split(",\\s*");
        this.displayedMetricsSet.addAll(Arrays.asList(displayedMetrics));
    }

    public static Logger getLogger() {
        return logger;
    }

    public Boolean getDisplayUserDefinedMetrics() {
        return this.displayUserDefinedMetrics;
    }

    public void setDisplayUserDefinedMetrics(Boolean displayUserDefinedMetrics) {
        this.displayUserDefinedMetrics = displayUserDefinedMetrics;
    }

    public Boolean getSinkAllMetrics() {
        return this.sinkAllMetrics;
    }

    public void setSinkAllMetrics(Boolean sinkAllMetrics) {
        this.sinkAllMetrics = sinkAllMetrics;
    }

    public Boolean getOutputNormalizedMetrics() {
        return this.outputNormalizedMetrics;
    }

    public void setOutputNormalizedMetrics(Boolean outputNormalizedMetrics) {
        this.outputNormalizedMetrics = outputNormalizedMetrics;
    }
}
