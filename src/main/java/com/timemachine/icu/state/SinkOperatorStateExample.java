package com.timemachine.icu.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class SinkOperatorStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 200ms 做一次 checkpoint
        env.enableCheckpointing(200);
        // 精确一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // job 被取消后，保留 checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // checkpoint 路径
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zaoshu/PersonalProject/daily_flink/src/main/resources/SinkOperatorStateExample");

        DataStreamSource<Tuple2<String, Integer>> datastream = env.fromElements(Tuple2.of("1", 1), Tuple2.of("2", 2), Tuple2.of("3", 3)).setParallelism(1);

        datastream.addSink(new BufferingSink(2));

        env.execute("SinkOperatorStateExample");

    }


    public static class BufferingSink implements CheckpointedFunction, SinkFunction<Tuple2<String, Integer>> {

        private final int threshold;

        private transient ListState<Tuple2<String, Integer>> checkpointedState;

        private List<Tuple2<String, Integer>> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }


        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            for (Tuple2<String, Integer> element : bufferedElements) {
                checkpointedState.add(element);
            }

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                    new ListStateDescriptor<Tuple2<String, Integer>>(
                            "state",
                            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                                @Override
                                public TypeInformation<Tuple2<String, Integer>> getTypeInfo() {
                                    return super.getTypeInfo();
                                }
                            })
                    );
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                for (Tuple2<String, Integer> element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }

        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (Tuple2<String, Integer> element : bufferedElements) {
                    System.out.println(element);
                }
                bufferedElements.clear();
            }

        }
    }
}
