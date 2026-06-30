package org.example.stream.metrics.utils.extractor.storm;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.example.stream.metrics.config.JobConfig;
import org.example.stream.metrics.utils.GsonUtil;
import org.example.stream.metrics.utils.extractor.MetricsUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormUserDefinedMetricsUtil extends MetricsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StormUserDefinedMetricsUtil.class);

    public StormUserDefinedMetricsUtil() {
    }

    public void init(JobConfig jobConfig, String delimiter) {
        this.setMetricsName(this.getFullName() + delimiter + ((JsonObject)this.getValue()).get("metricType").getAsString());
        this.setType("storm_user_defined");

        try {
            Object valueObject = ((JsonObject)this.getValue()).get("value");
            List<MetricsUtil.MetricsBean> userMetricsBeanList = new ArrayList();
            this.setValue(valueObject);
            if (this.validator()) {
                MetricsBean bean = new MetricsBean();
                bean.setName(this.getMetricsName());
                bean.setValue(this.getValue());
                bean.setTags(new HashMap());
                bean.getTags().put("user-defined-key", bean.getName());
                userMetricsBeanList.add(bean);
            } else {
                Map<String, Object> valueMap = (Map)GsonUtil.fromJson((JsonObject)valueObject, (new TypeToken<Map<String, Object>>() {
                }).getType());

                for(Map.Entry<String, Object> userDefinedMetrics : valueMap.entrySet()) {
                    MetricsBean bean = new MetricsBean();
                    bean.setName(this.getMetricsName());
                    bean.setValue(userDefinedMetrics.getValue());
                    bean.setTags(new HashMap());
                    bean.getTags().put("user-defined-key", userDefinedMetrics.getKey());
                    userMetricsBeanList.add(bean);
                }
            }

            this.setValue(userMetricsBeanList);
            this.setValid(true);
            this.setTags(new HashMap());
        } catch (Exception e) {
            this.setValid(false);
            logger.error("Metrics not valid: " + this.getFullName() + " - " + this.getValue() + "\nwith Exception: ", e);
        }

    }
}
