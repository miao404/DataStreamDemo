package org.example.stream.metrics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.Instant;

public class StateJoinJob {
    private static final Logger logger = LoggerFactory.getLogger(StateJoinJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SampleLogEvent> inputStream1 = generateSampleLog(env);

        DataStream<SampleLogEvent> inputStream2 = generateSampleLog(env);

        // simulate stream1 --> keyby --> keyedCoprocess->printSink
        //          stream2 --> keyby  ./
        SingleOutputStreamOperator<String> coProcess =
                inputStream1.keyBy(SampleLogEvent::joinKey)
                        .connect(inputStream2.keyBy(SampleLogEvent::joinKey)
                        ).process(
                                new SampleJoinProcessFunction(
                                        Time.minutes(20),Time.minutes(20), false,100));

        coProcess.addSink(new PrintSinkFunction<String>()).disableChaining();

        env.execute();
    }

    public static DataStream<SampleLogEvent> generateSampleLog(StreamExecutionEnvironment env) {
        DataStream<SampleLogEvent> inputStream = env.addSource(new ParallelSourceFunction<SampleLogEvent>() {
            private boolean running = true;
            @Override
            public void run(SourceContext<SampleLogEvent> sourceContext) throws Exception {
                while (running) {
                    long startTime = System.currentTimeMillis();
                    synchronized (sourceContext.getCheckpointLock()) {
                        for (int i = 0; i < 5000000; i++) {
                            sourceContext.collect(createSampleEvent());
                        }
                    }
                    long millisToSleep = 1000 - (System.currentTimeMillis() - startTime);
                    if (millisToSleep > 0) {
                        Thread.sleep(millisToSleep);
                    } else {
                        logger.warn(
                                "Slow data generation: cost {} milliseconds, check interval: {}",
                                1000 - millisToSleep,
                                1000);
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        return inputStream;
    }

    public static SampleLogEvent createSampleEvent() throws IllegalAccessException {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        // 转换为时间戳（毫秒）
        long timestampMillis = Instant.from(now).toEpochMilli();
        SampleLogEvent res = new SampleLogEvent(timestampMillis);
        res.setSampleFlowInfo(Gen7KBData.genDataSimple());
        return res;
    }
}
