package com.meituan.data.rt.metrics.function;

import com.meituan.data.rt.metrics.bean.OpenTsdbMetric;
import com.meituan.data.rt.metrics.config.MetricsToPetraConfig;
import java.util.Set;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

public class MetricsFilterFunction extends RichFilterFunction<OpenTsdbMetric> {
    final String METRICS_FILTER_FIELD = "metric";
    Set<String> displayedMetricsSet;
    MetricsToPetraConfig config;

    public MetricsFilterFunction() {
    }

    public MetricsFilterFunction(MetricsToPetraConfig jobConfig) {
        this.config = jobConfig;
        this.displayedMetricsSet = jobConfig.getDisplayedMetricsSet();
    }

    public void open(Configuration config) {
    }

    public boolean filter(OpenTsdbMetric metric) {
        if (this.displayedMetricsSet.contains(metric.getMetric())) {
            return true;
        } else {
            return this.config.getDisplayUserDefinedMetrics() ? metric.getTags().containsKey("metric_histogram") : false;
        }
    }
}
