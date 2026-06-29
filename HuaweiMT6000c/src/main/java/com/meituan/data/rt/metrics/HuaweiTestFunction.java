package com.meituan.data.rt.metrics;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HuaweiTestFunction extends MvIncrementProcessFunction<String> {

    private final static Logger logger = LoggerFactory.getLogger(HuaweiTestFunction.class);

    public HuaweiTestFunction(Time joinDuration, Time deduplicateDuration,
                              boolean enableListDirectEmitBorder, int listDirectEmitBorderSize) {
        super(joinDuration, deduplicateDuration, enableListDirectEmitBorder, listDirectEmitBorderSize);
    }

    // usages 2 implementations
    protected void actionOnMatched(PVMVLogType leftData, PVMVLogType rightData, Collector<String> out) {
        if (leftData.joinKey().equals(rightData.joinKey())) {
            logger.info("coprocess match {} and {} ", leftData, rightData);
        }
    }

    // usage 2 implementations
    protected void actionOnDirectEmit(PVMVLogType leftData, Collector<String> out) {
        logger.info("direct emit {}", leftData);
    }

    // usage 2 implementations
    protected void actionOnLeftDataExpired(PVMVLogType leftData, Collector<String> out, long watermark) {
        logger.info("left expired {}", leftData);
    }
}
