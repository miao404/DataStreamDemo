package org.example.stream.metrics.utils;

import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtil {
    private static final Logger logger = LoggerFactory.getLogger(KafkaUtil.class);

    public KafkaUtil() {
    }

    public static Pattern endWithPattern(String suffix) {
        return Pattern.compile(".*" + Pattern.quote(suffix) + "$");
    }

    public static Pattern notEndWithPattern(String suffix) {
        return Pattern.compile("(?!.*" + Pattern.quote(suffix) + "$).*");
    }

    public static Pattern startWithPattern(String prefix) {
        return Pattern.compile("^" + Pattern.quote(prefix) + ".*");
    }
}
