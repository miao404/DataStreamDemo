package com.meituan.data.rt.metrics.utils.extractor.flink;

import com.meituan.data.rt.metrics.config.JobConfig;
import com.meituan.data.rt.metrics.config.MetricsToPetraConfig;
import com.meituan.data.rt.metrics.utils.GsonUtil;
import com.meituan.data.rt.metrics.utils.extractor.MetricsUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkMetricsUtil extends MetricsUtil {
    private static final Logger logger = LoggerFactory.getLogger(FlinkMetricsUtil.class);

    public FlinkMetricsUtil() {
    }

    public void init(JobConfig config, String delimiter) {
        String[] splitedFields = this.getFullName().split(Pattern.quote(delimiter));
        this.setType(splitedFields[0]);
        List<String> tagNames = (List)((MetricsToPetraConfig)config).getScopeTagsMap().get(splitedFields[1]);
        Map<String, String> tags = new HashMap();

        for(int i = 0; i < tagNames.size(); ++i) {
            tags.put(tagNames.get(i), splitedFields[i + 1]);
        }

        this.setTags(tags);
        int metricsNameIndex = tagNames.size() + 1;
        List<String> rocksdbTags = ((MetricsToPetraConfig)config).getRocksdbExtraTags();
        if (splitedFields.length - metricsNameIndex >= rocksdbTags.size() + 2 && splitedFields[splitedFields.length - 1].startsWith("rocksdb.")) {
            ++metricsNameIndex;

            for(int i = 0; i < rocksdbTags.size(); ++i) {
                tags.put(rocksdbTags.get(i), splitedFields[metricsNameIndex + i]);
            }

            metricsNameIndex += rocksdbTags.size();
        } else if (splitedFields.length - metricsNameIndex >= rocksdbTags.size() + 2 && (splitedFields[splitedFields.length - 1].startsWith("aggregatingState") || splitedFields[splitedFields.length - 1].startsWith("listState") || splitedFields[splitedFields.length - 1].startsWith("mapState") || splitedFields[splitedFields.length - 1].startsWith("reducingState") || splitedFields[splitedFields.length - 1].startsWith("valueState"))) {
            for(int i = 0; i < rocksdbTags.size(); ++i) {
                tags.put(rocksdbTags.get(i), splitedFields[metricsNameIndex + 1 + i]);
            }

            metricsNameIndex += rocksdbTags.size() + 1;
        } else if (splitedFields.length - metricsNameIndex >= rocksdbTags.size() + 1 && (splitedFields[splitedFields.length - 1].startsWith("aggregatingState") || splitedFields[splitedFields.length - 1].startsWith("listState") || splitedFields[splitedFields.length - 1].startsWith("mapState") || splitedFields[splitedFields.length - 1].startsWith("reducingState") || splitedFields[splitedFields.length - 1].startsWith("valueState"))) {
            for(int i = 0; i < rocksdbTags.size(); ++i) {
                tags.put(rocksdbTags.get(i), splitedFields[metricsNameIndex + i]);
            }

            metricsNameIndex += rocksdbTags.size();
        }

        StringBuilder metricsNameStringBuilder = new StringBuilder();
        String metricsNameDelimiter = ((MetricsToPetraConfig)config).getMetricsNameDelimiter();
        if (splitedFields[metricsNameIndex].equals("KafkaConsumer")) {
            if (metricsNameIndex + 2 >= splitedFields.length) {
                this.setValid(false);
                return;
            }

            try {
                metricsNameStringBuilder.append(splitedFields[metricsNameIndex]).append(metricsNameDelimiter).append(splitedFields[metricsNameIndex + 1]);
                Map<String, String> kafkaConsumerTags = new HashMap();
                kafkaConsumerTags.put("job_name", tags.get("job_name"));
                kafkaConsumerTags.put("operator_name", tags.get("operator_name"));
                kafkaConsumerTags.put("scope", tags.get("scope"));
                int topicIndex = splitedFields[metricsNameIndex + 2].lastIndexOf(45);
                if (topicIndex < 0) {
                    this.setValid(false);
                    return;
                }

                kafkaConsumerTags.put("topic", splitedFields[metricsNameIndex + 2].substring(0, topicIndex));
                kafkaConsumerTags.put("partition", splitedFields[metricsNameIndex + 2].substring(topicIndex + 1));
                this.setTags(kafkaConsumerTags);
            } catch (Exception e) {
                logger.error("KafkaConsumer Metrics Error: {}", GsonUtil.toJson(this));
                logger.error("KafkaConsumer Metrics Error: ", e);
            }
        } else {
            for(int i = metricsNameIndex; i < splitedFields.length; ++i) {
                metricsNameStringBuilder.append(splitedFields[i]);
                if (i < splitedFields.length - 1) {
                    metricsNameStringBuilder.append(metricsNameDelimiter);
                }
            }
        }

        this.setMetricsName(metricsNameStringBuilder.toString());
        this.setValid(this.validator());
    }
}
