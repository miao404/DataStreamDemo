package org.example.stream.metrics.utils.extractor.storm;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.example.stream.metrics.config.JobConfig;
import org.example.stream.metrics.utils.GsonUtil;
import org.example.stream.metrics.utils.extractor.MetricsUtil;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormMapSumMetricsUtil extends MetricsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StormMapSumMetricsUtil.class);

    public StormMapSumMetricsUtil() {
    }

    public void init(JobConfig jobConfig, String delimiter) {
        this.setMetricsName(this.getFullName());
        this.setType("storm_map_sum");
        Double sum = (double)0.0F;

        try {
            Map<String, Object> valueMap = (Map)GsonUtil.fromJson((JsonObject)this.getValue(), (new TypeToken<Map<String, Object>>() {
            }).getType());

            for(Map.Entry<String, Object> entry : valueMap.entrySet()) {
                if (!((String)entry.getKey()).startsWith("__")) {
                    sum = sum + (Double)valueMap.get(entry.getKey());
                }
            }

            this.setValid(true);
            this.setValue(sum);
            this.setTags(new HashMap());
        } catch (Exception e) {
            this.setValid(false);
            logger.error("Metrics not valid: " + this.getFullName() + " - " + this.getValue() + "\nwith Exception: ", e);
        }

    }
}
