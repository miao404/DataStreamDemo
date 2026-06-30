package org.example.stream.metrics.function;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsGenerateSource extends RichParallelSourceFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsGenerateSource.class);
    private static final String JOB_NAME = "job_name";
    private static final String JOB_ID = "job_id";
    private static final String HOST_NAME = "host_name";
    private static final String APP_ID = "app_id";
    private static final String TM_ID = "tm_id";
    private static final String ENGINE_TYPE = "engine_type";
    private static final String ENGINE_VERSION = "engine_version";
    private static final String REPORT_INTERVAL = "report_interval";
    private static final String DELIMITER = "delimiter";
    private static final String TIMESTAMP = "timestamp";
    private static final String METRICS = "metrics";
    private static final String METRIC_NAME = "name";
    private static final String METRIC_VALUE = "value";
    private static final String REPORT_TIME = "2000-01-01 00:00:00.000";
    private static final String MACHINE = "sample-node-0001";
    private static final String ENGINE = "FLINK";
    private static final String ENGINE_NAME = "flink";
    private static final String ENGINE_VERSION_VALUE = "1.16.1";
    private static final String REPORT_THREAD = "AsyncMetricsAppender";
    private static final String REPORT_CLASS = "org.example.metrics.SampleMetricsReporter";
    private static final String SEPARATOR = "#XMDT#";
    private static final String LOG_LEVEL = "[INFO]";
    private static final String SPACE = " ";
    private static final String METRICS_DELIMITER_VALUE = "$";
    private static final String SAMPLE_JOB_NAME = "sample-stream-job";
    private static final int SAMPLE_JOB_ID = 1001;
    private static final String SAMPLE_APP_ID = "sample-session-0001";
    private static final String SAMPLE_TM_ID = "sample-session-taskmanager-2";
    private static final String SAMPLE_CONTAINER_ID = "sample-session-taskmanager-1";
    private static final String SAMPLE_TIMESTAMP = "946684800";
    private static final String COUNTER_TYPE = "Counter";
    private static final String GAUGE_TYPE = "Gauge";
    private static final String METER_TYPE = "Meter";
    private static final String HISTOGRAM_TYPE = "Histogram";
    private static final Map<String, Object> INFORMATION = new HashMap<String, Object>() {
        {
            this.put(JOB_NAME, SAMPLE_JOB_NAME);
            this.put("jobID", SAMPLE_JOB_ID);
            this.put("applicationID", SAMPLE_APP_ID);
            this.put("containerID", SAMPLE_CONTAINER_ID);
        }
    };
    private static final String INFORMATION_STR;
    public static Properties metricsWithType;
    private volatile boolean running = true;
    private int numPerCheck;
    private int checkInterval;
    private int lenPerStr;
    private int lenRange;
    private boolean isLenRandom;

    public MetricsGenerateSource() {
    }

    public MetricsGenerateSource(int numPerCheck, int checkInterval, int lenPerStr, int lenRange, boolean isLenRandom) {
        this.numPerCheck = numPerCheck;
        this.checkInterval = checkInterval;
        this.lenPerStr = lenPerStr;
        this.lenRange = lenRange;
        this.isLenRandom = isLenRandom;
    }

    public void run(SourceFunction.SourceContext<String> sourceContext) throws Exception {
        while(this.running) {
            long startTime = System.currentTimeMillis();
            synchronized(sourceContext.getCheckpointLock()) {
                for(int i = 0; i < this.numPerCheck; ++i) {
                    sourceContext.collect(this.createMetricsStr());
                }
            }

            long millisToSleep = (long)this.checkInterval - (System.currentTimeMillis() - startTime);
            if (millisToSleep > 0L) {
                Thread.sleep(millisToSleep);
            } else {
                LOG.warn("Slow data generation: cost {} milliseconds, check interval: {}", (long)this.checkInterval - millisToSleep, this.checkInterval);
            }
        }

    }

    public void cancel() {
        this.running = false;
    }

    public String createMetricsStr() {
        StringBuilder metricsStr = new StringBuilder();
        metricsStr.append(REPORT_TIME).append(SPACE);
        metricsStr.append(MACHINE).append(SPACE);
        metricsStr.append(ENGINE).append(SPACE);
        metricsStr.append(LOG_LEVEL).append(SPACE);
        metricsStr.append(REPORT_THREAD).append(SPACE);
        metricsStr.append(REPORT_CLASS).append(SPACE);
        metricsStr.append(SEPARATOR).append(SPACE);
        metricsStr.append(INFORMATION_STR).append(SPACE);
        metricsStr.append(SEPARATOR).append(SPACE);
        String metrics = this.generateMetrics();
        metricsStr.append(metrics);
        return metricsStr.toString();
    }

    public String generateMetrics() {
        Map<String, Object> map = this.getPrefixMap();
        Random random = new Random();
        int counter = random.nextInt(Integer.MAX_VALUE);
        double gauge = random.nextDouble();
        double meter = random.nextDouble();
        double histogramValue = random.nextDouble();
        Map<String, Double> histogram = new HashMap();
        histogram.put("min", histogramValue);
        histogram.put("max", histogramValue);
        histogram.put("mean", histogramValue);
        histogram.put("p99", histogramValue);
        histogram.put("p98", histogramValue);
        histogram.put("p95", histogramValue);
        histogram.put("p75", histogramValue);
        histogram.put("p50", histogramValue);
        List<Map<String, Object>> metricsWithValue = new ArrayList();

        for(Map.Entry<Object, Object> metric : metricsWithType.entrySet()) {
            if (metric.getValue().equals(COUNTER_TYPE)) {
                Map<String, Object> metricUnit = new HashMap();
                metricUnit.put(METRIC_NAME, metric.getKey());
                metricUnit.put(METRIC_VALUE, counter);
                metricsWithValue.add(metricUnit);
            } else if (metric.getValue().equals(GAUGE_TYPE)) {
                Map<String, Object> metricUnit = new HashMap();
                metricUnit.put(METRIC_NAME, metric.getKey());
                metricUnit.put(METRIC_VALUE, gauge);
                metricsWithValue.add(metricUnit);
            } else if (metric.getValue().equals(METER_TYPE)) {
                Map<String, Object> metricUnit = new HashMap();
                metricUnit.put(METRIC_NAME, metric.getKey());
                metricUnit.put(METRIC_VALUE, meter);
                metricsWithValue.add(metricUnit);
            } else if (metric.getValue().equals(HISTOGRAM_TYPE)) {
                Map<String, Object> metricUnit = new HashMap();
                metricUnit.put(METRIC_NAME, metric.getKey());
                metricUnit.put(METRIC_VALUE, histogram);
                metricsWithValue.add(metricUnit);
            } else {
                LOG.error("error with generate metrics");
            }
        }

        if (this.isLenRandom) {
            int randomRange = random.nextInt(this.lenRange * 2 + 1);
            int numToRemove = metricsWithType.size() - (this.lenPerStr + this.lenRange - randomRange);
            if (numToRemove < metricsWithValue.size()) {
                for(int i = 0; i < numToRemove; ++i) {
                    int indexRemoved = random.nextInt(metricsWithValue.size());
                    metricsWithValue.remove(indexRemoved);
                }
            }
        }

        map.put(METRICS, metricsWithValue);
        Gson gson = (new GsonBuilder()).create();
        return gson.toJson(map);
    }

    private Map<String, Object> getPrefixMap() {
        Map<String, Object> map = new HashMap();
        map.put(JOB_NAME, SAMPLE_JOB_NAME);
        map.put(JOB_ID, SAMPLE_JOB_ID);
        map.put(HOST_NAME, MACHINE);
        map.put(APP_ID, SAMPLE_APP_ID);
        map.put(TM_ID, SAMPLE_TM_ID);
        map.put(ENGINE_TYPE, ENGINE_NAME);
        map.put(ENGINE_VERSION, ENGINE_VERSION_VALUE);
        map.put(REPORT_INTERVAL, REPORT_INTERVAL);
        map.put(DELIMITER, METRICS_DELIMITER_VALUE);
        map.put(TIMESTAMP, SAMPLE_TIMESTAMP);
        return map;
    }

    static {
        Gson gson = (new GsonBuilder()).create();
        INFORMATION_STR = gson.toJson(INFORMATION);
        metricsWithType = new Properties();

        try {
            InputStream inputStream = MetricsGenerateSource.class.getClassLoader().getResourceAsStream("flinkMetrics.properties");
            metricsWithType.load(inputStream);
        } catch (IOException e) {
            LOG.error("Unable to load metrics from flinkMetrics.properties!" + e);
        }

    }
}
