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
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormMapKafkaOffsetMetricsUtil extends MetricsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StormMapKafkaOffsetMetricsUtil.class);

    public StormMapKafkaOffsetMetricsUtil() {
    }

    public void init(JobConfig jobConfig, String delimiter) {
        String fullName = this.getFullName();
        if (fullName.contains(".")) {
            fullName = fullName.substring(fullName.lastIndexOf(46) + 1);
        }

        if (fullName.contains(delimiter)) {
            fullName = fullName.substring(fullName.lastIndexOf(delimiter) + 1);
        }

        this.setMetricsName(fullName);
        this.setType("storm_map_kafka_offset");

        try {
            List<MetricsUtil.MetricsBean> kafkaOffsetMetricsBeanList = new ArrayList();
            Map<String, Object> valueMap = (Map)GsonUtil.fromJson((JsonObject)this.getValue(), (new TypeToken<Map<String, Object>>() {
            }).getType());

            for(Map.Entry<String, Object> offset : valueMap.entrySet()) {
                String[] tags = ((String)offset.getKey()).split("/");
                MetricsBean bean = new MetricsBean();
                bean.setName(fullName + delimiter + tags[tags.length - 1]);
                bean.setValue(offset.getValue());
                bean.setTags(new HashMap());
                if (tags[tags.length - 1].startsWith("total")) {
                    if (tags.length == 3) {
                        tags[0] = tags[0] + "/" + tags[1];
                    }

                    bean.getTags().put("topic", tags[0]);
                } else {
                    if (tags.length == 4) {
                        tags[0] = tags[0] + "/" + tags[1];
                    }

                    bean.getTags().put("topic", tags[0]);
                    bean.getTags().put("partition", tags[tags.length - 2].split(Pattern.quote("_"))[1]);
                }

                kafkaOffsetMetricsBeanList.add(bean);
            }

            this.setValue(kafkaOffsetMetricsBeanList);
            this.setValid(true);
            this.setTags(new HashMap());
        } catch (Exception e) {
            this.setValid(false);
            logger.error("Metrics not valid: " + this.getFullName() + " - " + this.getValue() + "\nwith Exception: ", e);
        }

    }
}
