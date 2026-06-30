package org.example.stream.metrics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcessFunctionExample {
    public static void test(String[] args) throws Exception {
        // 创建 Flink 流处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从内存生成数据（这里生成一个简单的包含数字的流）
        DataStream<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 将数据转换为 Tuple2，方便后面按 key 分组
        DataStream<Tuple2<String, Integer>> mappedStream = source.map(new MapFunction<Integer, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Integer value) throws Exception {
                return new Tuple2<>("key", value);
            }
        });

        // 按 key 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mappedStream.keyBy(t -> t.f0);

        // 使用 KeyedProcessFunction
        DataStream<String> resultStream = keyedStream.process(new MyKeyedProcessFunction());

        // 将结果输出到屏幕
        resultStream.print();

        // 执行 Flink 流处理作业
        env.execute("KeyedProcessFunction Example");
    }

    // 自定义 KeyedProcessFunction
    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {
        // 定义一个定时器触发时间间隔（单位：毫秒）
        private static final long TIMER_INTERVAL = 5000;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            // 获取当前元素的值
            int currentValue = value.f1;

            // 输出当前处理的值
            out.collect("Processing value: " + currentValue);

            // 注册一个定时器，在 5 秒后触发
            ctx.timerService().registerEventTimeTimer(currentValue * TIMER_INTERVAL);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发时的处理逻辑
            out.collect("Timer triggered for key: " + ctx.getCurrentKey() + " at timestamp: " + timestamp);
        }
    }
}