package org.example.stream.metrics.utils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.example.stream.metrics.config.JobConfig;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;

public class GsonUtil {
    private static final Gson gson = new Gson();

    public GsonUtil() {
    }

    public static String toJson(Configuration config) {
        return gson.toJson(config);
    }

    public static String toJson(JobConfig jobConfig) {
        return gson.toJson(jobConfig);
    }

    public static String toJson(Map<String, String> map) {
        return gson.toJson(map);
    }

    public static <T> T fromJson(JsonObject jsonObject, Type typeOfT) {
        return null;
    }

    public static <T> T fromJson(String jsonStr, Type typeOfT) {
        return null;
    }

    public static String toJson(List<String> list) {
        return gson.toJson(list);
    }

    public static String toJson(JsonElement element) {
        return gson.toJson(element);
    }

    public static <R> String toJson(R collect) {
        return null;
    }

    public static JsonObject toJsonObject(String json) {
        JsonElement jsonElement = JsonParser.parseString(json);
        return jsonElement.getAsJsonObject();
    }
}