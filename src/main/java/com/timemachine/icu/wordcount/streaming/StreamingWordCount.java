package com.timemachine.icu.wordcount.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<String> text = env.socketTextStream("localhost", 8888);

        DataStream<Tuple2<String, Integer>> counts = text.
                flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] values = s.toLowerCase().split(" ");

                        for (String value : values) {
                            if (value.length() > 0) {
                                collector.collect(new Tuple2<String, Integer>(value, 1));
                            }
                        }

                    }
                })
                .keyBy(0)
                .sum(1);
         counts.print();
         env.execute("java com.timemachine.icu.wordcount.streaming word count");


    }
}
