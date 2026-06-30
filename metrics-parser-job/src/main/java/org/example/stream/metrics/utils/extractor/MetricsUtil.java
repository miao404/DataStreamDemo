package org.example.stream.metrics.utils.extractor;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.example.stream.metrics.config.JobConfig;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class MetricsUtil {
    private static final Pattern pattern = Pattern.compile("^[-+]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][-+]?\\d+)?[dD]?$");
    private String fullName;
    private Object value;
    private Boolean valid;
    private String metricsName;
    private String type;
    private Map<String, String> tags;

    public MetricsUtil() {
    }

    public MetricsUtil(JsonObject jsonObject) {
        this.setFullName(jsonObject.get("name").getAsString());
        this.setValue(jsonObject.get("value").getAsJsonObject());
    }

    public abstract void init(JobConfig var1, String var2);

    public Boolean validator() {
        if (this.getValue() != null && pattern.matcher(this.getValue().toString()).find()) {
            return true;
        } else if (this.getMetricsName().equals("isBackPressured") && ((JsonPrimitive)this.getValue()).getAsBoolean() == Boolean.TRUE) {
            this.setValue(1);
            return true;
        } else if (this.getMetricsName().equals("isBackPressured") && ((JsonPrimitive)this.getValue()).getAsBoolean() == Boolean.FALSE) {
            this.setValue(0);
            return true;
        } else {
            return false;
        }
    }

    public String getFullName() {
        return this.fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public Object getValue() {
        return this.value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Boolean getValid() {
        return this.valid;
    }

    public void setValid(Boolean valid) {
        this.valid = valid;
    }

    public String getMetricsName() {
        return this.metricsName;
    }

    public void setMetricsName(String metricsName) {
        this.metricsName = metricsName;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getTags() {
        return this.tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public class MetricsBean {
        private String name;
        private Map<String, String> tags;
        private Object value;

        public MetricsBean() {
        }

        public String getName() {
            return this.name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Map<String, String> getTags() {
            return this.tags;
        }

        public void setTags(Map<String, String> tags) {
            this.tags = tags;
        }

        public Object getValue() {
            return this.value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }
}
