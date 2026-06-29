package com.meituan.data.rt.metrics.utils.extractor.storm;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.meituan.data.rt.metrics.config.JobConfig;
import com.meituan.data.rt.metrics.utils.GsonUtil;
import com.meituan.data.rt.metrics.utils.extractor.MetricsUtil;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormMapAvgMetricsUtil extends MetricsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StormMapAvgMetricsUtil.class);

    public StormMapAvgMetricsUtil() {
    }

    public void init(JobConfig jobConfig, String delimiter) {
        this.setMetricsName(this.getFullName());
        this.setType("storm_map_avg");
        Double sum = (double)0.0F;
        Integer count = 0;

        try {
            Map<String, Object> valueMap = (Map)GsonUtil.fromJson((JsonObject)this.getValue(), (new TypeToken<Map<String, Object>>() {
            }).getType());

            for(Map.Entry<String, Object> entry : valueMap.entrySet()) {
                if (!((String)entry.getKey()).startsWith("__")) {
                    sum = sum + (Double)valueMap.get(entry.getKey());
                    count = count + 1;
                }
            }

            this.setValid(true);
            if (count > 0) {
                this.setValue(sum / (double)count);
            } else {
                this.setValue((double)0.0F);
            }

            this.setTags(new HashMap());
        } catch (Exception e) {
            this.setValid(false);
            logger.error("Metrics not valid: " + this.getFullName() + " - " + this.getValue() + "\nwith Exception: ", e);
        }

    }
}
