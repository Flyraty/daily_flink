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
import sun.jvm.hotspot.runtime.Thread;

import java.util.Arrays;
import java.util.List;

/**
 * 自定义 PunctuatedWatermarkGenerator
 */
public class PunctuatedWatermarkGeneratorExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9902);

        DataStream<Tuple2<String, Long>> resultStream = socketStream
                .assignTimestampsAndWatermarks(new WatermarkStrategy<String>() {
                    @Override
                    public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyPunctuatedWatermarkGenerator();
                    }
                }.withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        long eventTime = Long.parseLong(element.split(" ")[0]);
                        return eventTime;
                    }
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


    private static class MyPunctuatedWatermarkGenerator implements WatermarkGenerator<String> {

        private List<String> monitorEvents = Arrays.asList("a", "b");

        @Override
        public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
            String eventMarker = event.split(" ")[1];
            if (monitorEvents.contains(eventMarker)) {
                long eventTimestamps = Long.parseLong(event.split(" ")[0]);
                Watermark watermark = new Watermark(eventTimestamps);
                System.out.println(String.format("systemTime:%s, Event:%s", Long.toString(System.currentTimeMillis()), event));
                output.emitWatermark(watermark);
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}
