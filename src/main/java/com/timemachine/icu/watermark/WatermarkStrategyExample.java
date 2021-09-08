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

import java.time.Duration;


/**
 * watermark 测试，10s 的固定窗口，3s 的固定延迟。
 */
public class WatermarkStrategyExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9900);

        DataStream<Tuple2<String, Long>> resultStream;
        resultStream = socketStream
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((SerializableTimestampAssigner<String>) (event, recordTimestamp) -> {
                    long eventTime = Long.parseLong(event.split(" ")[0]);
                    System.out.println(String.format("systemTime:%s, Event:%s", Long.toString(System.currentTimeMillis()), event));
                    return eventTime;
                })
            )
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
}


