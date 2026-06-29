package com.meituan.data.rt.metrics;

import com.meituan.data.rt.metrics.bean.OpenTsdbMetric;
import com.meituan.data.rt.metrics.bean.PetraMetric;
import com.meituan.data.rt.metrics.config.JobConfig;
import com.meituan.data.rt.metrics.config.MetricsToPetraConfig;
import com.meituan.data.rt.metrics.function.MetricsFilterFunction;
import com.meituan.data.rt.metrics.function.MetricsGenerateSource;
import com.meituan.data.rt.metrics.utils.GsonUtil;
import com.meituan.data.rt.metrics.utils.JobInitUtil;
import com.meituan.data.rt.metrics.utils.MetricsFormatterUtil;
import com.meituan.data.rt.metrics.utils.extractor.MetricsExtractor;
import com.meituan.data.rt.metrics.utils.extractor.flink.FlinkMetricsUtil;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsParser {
    private static final Logger logger = LoggerFactory.getLogger(MetricsParser.class);

    public MetricsParser() {
    }

    public static void main(String[] args) throws Exception {
        MetricsToPetraConfig jobConfig = new MetricsToPetraConfig();
        jobConfig.initFromConfiguration(ParameterTool.fromArgs(args).getConfiguration());
        StreamExecutionEnvironment env = JobInitUtil.initEnvFromConfig(jobConfig);
        DataStream<String> inputStream = generateMetricsStrStream(jobConfig, env);
        DataStream<OpenTsdbMetric> formattedStream = inputStream.flatMap(new MetricsToPetra(jobConfig)).name("metrics_formatter").setParallelism(jobConfig.getMetricsFormatterParallelism() > 0 ? jobConfig.getMetricsFormatterParallelism() : env.getParallelism());
        DataStream<String> filteredStrStream = formattedStream.filter(new MetricsFilterFunction(jobConfig)).map(new OpenTsdbMetric2String());
        printDetails(jobConfig, inputStream, filteredStrStream);
        env.execute();
    }

    public static DataStream<String> generateMetricsStrStream(JobConfig jobConfig, StreamExecutionEnvironment env) {
        MetricsGenerateSource generateSource = new MetricsGenerateSource(jobConfig.getNumPerCheck(), jobConfig.getCheckInterval(), jobConfig.getLenPerStr(), jobConfig.getLenRange(), jobConfig.getIsLenRandom());
        DataStream<String> inputStream;
        if (jobConfig.getIsSourceChaining()) {
            inputStream = env.addSource(generateSource).setParallelism(jobConfig.getSourceParallelism() > 0 ? jobConfig.getSourceParallelism() : env.getParallelism());
        } else {
            inputStream = env.addSource(generateSource).setParallelism(jobConfig.getSourceParallelism() > 0 ? jobConfig.getSourceParallelism() : env.getParallelism()).disableChaining();
        }

        return inputStream;
    }

    static void printDetails(JobConfig jobConfig, DataStream<String> inputStream, DataStream<String> resultStream) {
        boolean printSource = jobConfig.getIsSourcePrint();
        boolean printResult = jobConfig.getIsSinkPrint();
        if (printSource) {
            inputStream.flatMap(new FlatMapFunction<String, Object>() {
                public void flatMap(String record, Collector<Object> collector) throws Exception {
                    MetricsParser.logger.info(record);
                }
            });
        }

        if (printResult) {
            resultStream.flatMap(new FlatMapFunction<String, Object>() {
                public void flatMap(String resultRecord, Collector<Object> collector) throws Exception {
                    MetricsParser.logger.info(resultRecord);
                }
            });
        }

    }

    public static class MetricsToPetra extends RichFlatMapFunction<String, OpenTsdbMetric> {
        final String METRICS_BEGIN_IDENTIFIER = "#XMDT#";
        List<String> flinkJobCommonTags;
        FlinkMetricsUtil metricsUtil;
        MetricsToPetraConfig config;

        public MetricsToPetra() {
        }

        public MetricsToPetra(MetricsToPetraConfig jobConfig) {
            this.config = jobConfig;
            MetricsParser.logger.info("JobConfig: {}", GsonUtil.toJson(jobConfig));
            this.flinkJobCommonTags = jobConfig.getOpentsdbTags();
        }

        public void open(Configuration config) throws Exception {
            this.metricsUtil = new FlinkMetricsUtil();
            MetricsParser.logger.info(GsonUtil.toJson(config));
        }

        public void flatMap(String metricString, Collector<OpenTsdbMetric> collector) throws Exception {
            int index = metricString.lastIndexOf("#XMDT#");
            if (index == -1) {
                MetricsParser.logger.warn("Metrics Format Not Match: " + metricString);
            } else {
                MetricsExtractor extractor = new MetricsExtractor(metricString.substring(index + "#XMDT#".length()));
                if (extractor.getJson() != null) {
                    extractor.init(this.config);

                    for(PetraMetric petraMetric : extractor.getMetrics()) {
                        for(OpenTsdbMetric openTsdbMetric : MetricsFormatterUtil.petraToOpenTsdb(petraMetric)) {
                            collector.collect(openTsdbMetric);
                        }
                    }
                }

            }
        }
    }

    public static class MetricsFilter extends RichFilterFunction<OpenTsdbMetric> {
        final String METRICS_FILTER_FIELD = "metric";
        Set<String> displayedMetricsSet;
        MetricsToPetraConfig config;

        public MetricsFilter() {
        }

        public MetricsFilter(MetricsToPetraConfig jobConfig) {
            this.config = jobConfig;
            this.displayedMetricsSet = jobConfig.getDisplayedMetricsSet();
        }

        public void open(Configuration config) {
        }

        public boolean filter(OpenTsdbMetric metric) {
            return this.displayedMetricsSet.contains(metric.getMetric());
        }
    }

    public static class OpenTsdbMetric2String implements MapFunction<OpenTsdbMetric, String> {
        public OpenTsdbMetric2String() {
        }

        public String map(OpenTsdbMetric metric) throws Exception {
            return MetricsFormatterUtil.openTsdbToJsonString(metric);
        }
    }
}
