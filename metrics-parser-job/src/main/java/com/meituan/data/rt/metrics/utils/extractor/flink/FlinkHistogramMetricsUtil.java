package com.meituan.data.rt.metrics.utils.extractor.flink;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.meituan.data.rt.metrics.config.JobConfig;
import com.meituan.data.rt.metrics.config.MetricsToPetraConfig;
import com.meituan.data.rt.metrics.utils.extractor.MetricsUtil;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkHistogramMetricsUtil extends MetricsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkHistogramMetricsUtil.class);
    private static final String FLINK_HISTOGRAM_TAG = "metric_histogram";

    public FlinkHistogramMetricsUtil() {
    }

    public void init(JobConfig config, String delimiter) {
        String[] splitedFields = this.getFullName().split(Pattern.quote(delimiter));
        this.setType(splitedFields[0]);
        List<String> tagNames = (List)((MetricsToPetraConfig)config).getScopeTagsMap().get(splitedFields[1]);
        Map<String, String> tags = new HashMap();

        for(int i = 0; i < tagNames.size(); ++i) {
            tags.put(tagNames.get(i), splitedFields[i + 1]);
        }

        int metricsNameIndex = tagNames.size() + 1;
        String metricName = splitedFields[metricsNameIndex];
        this.setMetricsName(metricName);
        List<String> rocksdbTags = ((MetricsToPetraConfig)config).getRocksdbExtraTags();
        if (metricName.equals("state_name")) {
            ++metricsNameIndex;
            metricName = splitedFields[metricsNameIndex];
        }

        int stateMetricsNameIndex = metricsNameIndex + 1;
        if (stateMetricsNameIndex < splitedFields.length) {
            String stateMerticName = splitedFields[stateMetricsNameIndex];
            if ((stateMerticName.startsWith("aggregatingState") || stateMerticName.startsWith("listState") || stateMerticName.startsWith("mapState") || stateMerticName.startsWith("reducingState") || stateMerticName.startsWith("valueState")) && stateMerticName.endsWith("Latency")) {
                tags.put(rocksdbTags.get(0), metricName);
                this.setMetricsName(stateMerticName);
            }
        }

        List<String> extraTagNames = (List)((MetricsToPetraConfig)config).getExtraTagsMap().get(metricName);
        if (extraTagNames != null) {
            if (extraTagNames.size() == 1 && ((String)extraTagNames.get(0)).equals("self-contained")) {
                for(int selfContainedTagIndex = metricsNameIndex + 1; selfContainedTagIndex + 1 < splitedFields.length; selfContainedTagIndex += 2) {
                    tags.put(splitedFields[selfContainedTagIndex], splitedFields[selfContainedTagIndex + 1]);
                }
            } else {
                for(int i = 0; i < extraTagNames.size(); ++i) {
                    tags.put(extraTagNames.get(i), splitedFields[i + 1 + tagNames.size()]);
                }
            }
        }

        this.setTags(tags);

        try {
            List<MetricsUtil.MetricsBean> metricsBeans = new LinkedList();
            Map<String, Object> histogramMetric = (Map)(new Gson()).fromJson(this.getValue().toString(), (new TypeToken<HashMap<String, Object>>() {
            }).getType());

            for(Map.Entry<String, Object> entry : histogramMetric.entrySet()) {
                Object histogramMetricValue = entry.getValue();
                if (histogramMetricValue != null) {
                    MetricsBean bean = new MetricsBean();
                    Map<String, String> metricBeanTags = new HashMap();
                    metricBeanTags.put(FLINK_HISTOGRAM_TAG, entry.getKey());
                    bean.setTags(metricBeanTags);
                    bean.setName(this.getMetricsName());
                    bean.setValue(histogramMetricValue);
                    metricsBeans.add(bean);
                }
            }

            setValue(metricsBeans);
            setValid(true);
        } catch (Exception e) {
            LOG.error("Flink histogram metric parse failure", e);
        }

    }
}