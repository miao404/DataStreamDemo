package org.example.stream.metrics.utils;

import org.example.stream.metrics.config.MetricsParserConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JobInitUtil {
    public JobInitUtil() {
    }

    public static StreamExecutionEnvironment initEnvFromConfig(MetricsParserConfig jobConfig) {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}