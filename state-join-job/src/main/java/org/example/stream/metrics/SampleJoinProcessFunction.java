package org.example.stream.metrics;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleJoinProcessFunction extends IncrementalJoinProcessFunction<String> {

    private final static Logger logger = LoggerFactory.getLogger(SampleJoinProcessFunction.class);

    public SampleJoinProcessFunction(Time joinDuration, Time deduplicateDuration,
                              boolean enableListDirectEmitBorder, int listDirectEmitBorderSize) {
        super(joinDuration, deduplicateDuration, enableListDirectEmitBorder, listDirectEmitBorderSize);
    }

    // usages 2 implementations
    protected void actionOnMatched(SampleLogEvent leftData, SampleLogEvent rightData, Collector<String> out) {
        if (leftData.joinKey().equals(rightData.joinKey())) {
            logger.info("coprocess match {} and {} ", leftData, rightData);
        }
    }

    // usage 2 implementations
    protected void actionOnDirectEmit(SampleLogEvent leftData, Collector<String> out) {
        logger.info("direct emit {}", leftData);
    }

    // usage 2 implementations
    protected void actionOnLeftDataExpired(SampleLogEvent leftData, Collector<String> out, long watermark) {
        logger.info("left expired {}", leftData);
    }
}
