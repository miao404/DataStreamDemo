package org.example.stream.metrics.utils.extractor.storm;

import com.google.gson.JsonObject;
import org.example.stream.metrics.config.JobConfig;
import org.example.stream.metrics.utils.extractor.MetricsUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormMapGcMetricsUtil extends MetricsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StormMapGcMetricsUtil.class);

    public StormMapGcMetricsUtil() {
    }

    public void init(JobConfig jobConfig, String delimiter) {
        this.setMetricsName(this.getFullName());
        this.setType("storm_map_gc");

        try {
            JsonObject valueMap = (JsonObject)this.getValue();
            List<MetricsUtil.MetricsBean> gcMetricsBeanList = new ArrayList();
            MetricsBean gcCount = new MetricsBean();
            gcCount.setName(this.getFullName() + delimiter + "count");
            gcCount.setValue(valueMap.get("count"));
            gcCount.setTags(new HashMap());
            MetricsBean gcTime = new MetricsBean();
            gcTime.setName(this.getFullName() + delimiter + "timeMs");
            gcTime.setValue(valueMap.get("timeMs"));
            gcTime.setTags(new HashMap());
            gcMetricsBeanList.add(gcCount);
            gcMetricsBeanList.add(gcTime);
            this.setValue(gcMetricsBeanList);
            this.setValid(true);
            this.setTags(new HashMap());
        } catch (Exception e) {
            this.setValid(false);
            logger.error("Metrics not valid: " + this.getFullName() + " - " + this.getValue() + "\nwith Exception: ", e);
        }

    }
}
