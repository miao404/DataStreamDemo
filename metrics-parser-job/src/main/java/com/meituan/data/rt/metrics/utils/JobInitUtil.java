package com.meituan.data.rt.metrics.utils;

import com.meituan.data.rt.metrics.config.MetricsToPetraConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JobInitUtil {
    public JobInitUtil() {
    }

    public static StreamExecutionEnvironment initEnvFromConfig(MetricsToPetraConfig jobConfig) {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}