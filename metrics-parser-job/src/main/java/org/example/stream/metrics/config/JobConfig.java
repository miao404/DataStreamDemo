package org.example.stream.metrics.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobConfig implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(JobConfig.class);
    private String jobName;
    private int numPerCheck;
    private int checkInterval;
    private int lenPerStr;
    private int lenRange;
    private boolean isSourcePrint;
    private boolean isSinkPrint;
    private boolean isLenRandom;
    private boolean isSourceChaining;
    private int sourceParallelism;
    private int metricsFormatterParallelism;

    public JobConfig() {
        InputStream inputStream = JobConfig.class.getClassLoader().getResourceAsStream("config.properties");

        try {
            Configuration config = ParameterTool.fromPropertiesFile(inputStream).getConfiguration();
            this.initFromConfiguration(config);
        } catch (IOException e) {
            logger.error("Unable to load default config from config.properties!", e);
        }

    }

    protected void initFromConfiguration(Configuration config) {
        if (config.containsKey("stream.metrics.default.name")) {
            this.setJobName(config.getString("stream.metrics.default.name", "metrics_parser"));
        }

        if (config.containsKey("stream.metrics.num_per_check")) {
            this.setNumPerCheck(config.getInteger("stream.metrics.num_per_check", 1000));
        }

        if (config.containsKey("stream.metrics.check_interval")) {
            this.setCheckInterval(config.getInteger("stream.metrics.check_interval", 1000));
        }

        if (config.containsKey("stream.metrics.len_per_str")) {
            this.setLenPerStr(config.getInteger("stream.metrics.len_per_str", 50));
        }

        if (config.containsKey("stream.metrics.len_range")) {
            this.setLenRange(config.getInteger("stream.metrics.len_range", 10));
        }

        if (config.containsKey("stream.metrics.source.print")) {
            this.setIsSourcePrint(config.getBoolean("stream.metrics.source.print", false));
        } else {
            this.setIsSourcePrint(false);
        }

        if (config.containsKey("stream.metrics.sink.print")) {
            this.setIsSinkPrint(config.getBoolean("stream.metrics.sink.print", false));
        } else {
            this.setIsSinkPrint(false);
        }

        if (config.containsKey("stream.metrics.source.parallelism")) {
            this.setSourceParallelism(config.getInteger("stream.metrics.source.parallelism", -1));
        } else {
            this.setSourceParallelism(-1);
        }

        if (config.containsKey("stream.metrics.formatter.parallelism")) {
            this.setMetricsFormatterParallelism(config.getInteger("stream.metrics.formatter.parallelism", -1));
        } else {
            this.setMetricsFormatterParallelism(-1);
        }

        if (config.containsKey("stream.metrics.len_random")) {
            this.setIsLenRandom(config.getBoolean("stream.metrics.len_random", false));
        } else {
            this.setIsLenRandom(false);
        }

        if (config.containsKey("stream.metrics.source.chaining")) {
            this.setIsSourceChaining(config.getBoolean("stream.metrics.source.chaining", true));
        } else {
            this.setIsSourceChaining(true);
        }

    }

    protected Configuration toConfiguration() {
        Configuration config = new Configuration();
        config.setString("stream.metrics.default.name", this.getJobName());
        config.setInteger("stream.metrics.num_per_check", this.getNumPerCheck());
        config.setInteger("stream.metrics.check_interval", this.getCheckInterval());
        config.setInteger("stream.metrics.len_per_str", this.getLenPerStr());
        config.setInteger("stream.metrics.len_range", this.getLenRange());
        config.setBoolean("stream.metrics.source.print", this.getIsSourcePrint());
        config.setBoolean("stream.metrics.sink.print", this.getIsSinkPrint());
        config.setInteger("stream.metrics.source.parallelism", this.getSourceParallelism());
        config.setInteger("stream.metrics.formatter.parallelism", this.getMetricsFormatterParallelism());
        config.setBoolean("stream.metrics.len_random", this.getIsLenRandom());
        config.setBoolean("stream.metrics.source.chaining", this.getIsSourceChaining());
        return config;
    }

    public String getJobName() {
        return this.jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getNumPerCheck() {
        return this.numPerCheck;
    }

    public void setNumPerCheck(int numPerCheck) {
        this.numPerCheck = numPerCheck;
    }

    public int getCheckInterval() {
        return this.checkInterval;
    }

    public void setCheckInterval(int checkInterval) {
        this.checkInterval = checkInterval;
    }

    public int getLenPerStr() {
        return this.lenPerStr;
    }

    public void setLenPerStr(int lenPerStr) {
        this.lenPerStr = lenPerStr;
    }

    public int getLenRange() {
        return this.lenRange;
    }

    public void setLenRange(int lenRange) {
        this.lenRange = lenRange;
    }

    public boolean getIsSourcePrint() {
        return this.isSourcePrint;
    }

    public void setIsSourcePrint(boolean isSourcePrint) {
        this.isSourcePrint = isSourcePrint;
    }

    public boolean getIsSinkPrint() {
        return this.isSinkPrint;
    }

    public void setIsSinkPrint(boolean isSinkPrint) {
        this.isSinkPrint = isSinkPrint;
    }

    public int getSourceParallelism() {
        return this.sourceParallelism;
    }

    public void setSourceParallelism(int sourceParallelism) {
        this.sourceParallelism = sourceParallelism;
    }

    public int getMetricsFormatterParallelism() {
        return this.metricsFormatterParallelism;
    }

    public void setMetricsFormatterParallelism(int metricsFormatterParallelism) {
        this.metricsFormatterParallelism = metricsFormatterParallelism;
    }

    public boolean getIsLenRandom() {
        return this.isLenRandom;
    }

    public void setIsLenRandom(boolean isLenRandom) {
        this.isLenRandom = isLenRandom;
    }

    public boolean getIsSourceChaining() {
        return this.isSourceChaining;
    }

    public void setIsSourceChaining(boolean isSourceChaining) {
        this.isSourceChaining = isSourceChaining;
    }
}
