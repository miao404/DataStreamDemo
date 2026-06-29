package com.meituan.data.rt.metrics.function;

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
    private static final String REPORT_TIME = "2024-09-20 16:02:11.537";
    private static final String MACHINE = "flinkTest-node00045";
    private static final String ENGINE = "FLINK";
    private static final String REPORT_THREAD = "RTAsyncScribeAppender-metricScribe";
    private static final String REPORT_CLASS = "org.apache.flink.common.metric.FlinkMetricsReporter";
    private static final String SEPARATOR = "#XMDT#";
    private static final Map<String, Object> INFORMATION = new HashMap<String, Object>() {
        {
            this.put("job_name", "bigscale-job-test-k8s");
            this.put("jobID", 1482820);
            this.put("applicationID", "session-1482820-1726816351");
            this.put("containerID", "session-1482820-1726816351-taskmanager-1-530");
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
        metricsStr.append("2024-09-20 16:02:11.537").append(" ");
        metricsStr.append("flinkTest-node00045").append(" ");
        metricsStr.append("FLINK").append(" ");
        metricsStr.append("[INFO]").append(" ");
        metricsStr.append("RTAsyncScribeAppender-metricScribe").append(" ");
        metricsStr.append("org.apache.flink.common.metric.FlinkMetricsReporter").append(" ");
        metricsStr.append("#XMDT#").append(" ");
        metricsStr.append(INFORMATION_STR).append(" ");
        metricsStr.append("#XMDT#").append(" ");
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
            if (metric.getValue().equals("Counter")) {
                Map<String, Object> metricUnit = new HashMap();
                metricUnit.put("name", metric.getKey());
                metricUnit.put("value", counter);
                metricsWithValue.add(metricUnit);
            } else if (metric.getValue().equals("Gauge")) {
                Map<String, Object> metricUnit = new HashMap();
                metricUnit.put("name", metric.getKey());
                metricUnit.put("value", gauge);
                metricsWithValue.add(metricUnit);
            } else if (metric.getValue().equals("Meter")) {
                Map<String, Object> metricUnit = new HashMap();
                metricUnit.put("name", metric.getKey());
                metricUnit.put("value", meter);
                metricsWithValue.add(metricUnit);
            } else if (metric.getValue().equals("Histogram")) {
                Map<String, Object> metricUnit = new HashMap();
                metricUnit.put("name", metric.getKey());
                metricUnit.put("value", histogram);
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

        map.put("metrics", metricsWithValue);
        Gson gson = (new GsonBuilder()).create();
        return gson.toJson(map);
    }

    private Map<String, Object> getPrefixMap() {
        Map<String, Object> map = new HashMap();
        map.put("job_name", "bigscale-job-test-k8s");
        map.put("job_id", 1482820);
        map.put("host_name", "flinkTest-node00045");
        map.put("app_id", "session-1482820-1726816351");
        map.put("tm_id", "session-1482820-1726816351-taskmanager-1-1304");
        map.put("engine_type", "flink");
        map.put("engine_version", "1.16.1");
        map.put("report_interval", "report_interval");
        map.put("delimiter", "$");
        map.put("timestamp", "1726819335");
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
