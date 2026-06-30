package org.example.stream.metrics.utils.extractor.storm;

import com.google.gson.JsonObject;
import org.example.stream.metrics.config.JobConfig;
import org.example.stream.metrics.utils.extractor.MetricsUtil;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormMapMemoryMetricsUtil extends MetricsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StormMapMemoryMetricsUtil.class);

    public StormMapMemoryMetricsUtil() {
    }

    public void init(JobConfig jobConfig, String delimiter) {
        this.setMetricsName(this.getFullName() + delimiter + "usedBytes");
        this.setType("storm_map_memory");

        try {
            JsonObject valueMap = (JsonObject)this.getValue();
            this.setValue(valueMap.get("usedBytes"));
            this.setValid(this.validator());
            this.setTags(new HashMap());
        } catch (Exception e) {
            this.setValid(false);
            logger.error("Metrics not valid: " + this.getFullName() + " - " + this.getValue() + "\nwith Exception: ", e);
        }

    }
}
