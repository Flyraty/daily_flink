package com.timemachine.icu.watermark;


import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 自定义 PeriodicWatermarkGenerator
 */
public class PeriodicWatermarkGeneratorExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9901);

        DataStream<Tuple2<String, Long>> resultStream;
        resultStream = socketStream
                .assignTimestampsAndWatermarks(new WatermarkStrategy<String>() {
                    @Override
                    public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyBoundedOutOfOrdernessGenerator();
                    }
                }.withTimestampAssigner((SerializableTimestampAssigner<String>) (element, recordTimestamp) -> {
                    long eventTime = Long.parseLong(element.split(" ")[0]);
                    return eventTime;
                }))
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value.split(" ")[1], 1L);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });
        resultStream.print();
        env.execute();

    }

    private static class MyBoundedOutOfOrdernessGenerator implements WatermarkGenerator<String> {

        private final long maxOutOfOrderness = 3000; // 3s 延迟

        private long currentMaxTimestamp;

        @Override
        public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
            long eventTimestamps = Long.parseLong(event.split(" ")[0]);
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamps);
            System.out.println(String.format("systemTime:%s, Event:%s", Long.toString(System.currentTimeMillis()), event));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
        }
    }
}


