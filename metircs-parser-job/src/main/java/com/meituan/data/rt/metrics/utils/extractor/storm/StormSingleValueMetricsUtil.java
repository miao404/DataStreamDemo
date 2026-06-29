package com.meituan.data.rt.metrics.utils.extractor.storm;

import com.meituan.data.rt.metrics.config.JobConfig;
import com.meituan.data.rt.metrics.utils.extractor.MetricsUtil;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormSingleValueMetricsUtil extends MetricsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StormSingleValueMetricsUtil.class);

    public StormSingleValueMetricsUtil() {
    }

    public void init(JobConfig jobConfig, String delimiter) {
        this.setMetricsName(this.getFullName());
        this.setType("storm_single_value");

        try {
            this.setValid(this.validator());
            this.setTags(new HashMap());
        } catch (Exception e) {
            this.setValid(false);
            logger.error("Metrics not valid: " + this.getFullName() + " - " + this.getValue() + "\nwith Exception: ", e);
        }

    }
}
