package com.timemachine.icu.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SourceOperatorStateExample {

    private static final Logger LOG = LoggerFactory.getLogger(SourceOperatorStateExample.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 200ms 做一次 checkpoint
        env.enableCheckpointing(200);
        // job 被取消后，保留 checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 精确一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint 路径
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zaoshu/PersonalProject/daily_flink/src/main/resources/SourceOperatorStateExample");

        DataStream<Long> dataStream = env.addSource(new CounterSource());

        dataStream.print();

        env.execute();

    }

    public static class CounterSource
            extends RichParallelSourceFunction<Long>
            implements CheckpointedFunction {


        private Long offset = 0L;

        private volatile boolean isRunning = true;

        private ListState<Long> state;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(offset);

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<Long>(
                    "state",
                    LongSerializer.INSTANCE
            ));

            for (long l : state.get()) {
                offset = l;
            }

            LOG.info("restore offset: " + offset);


        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {

            final Object lock = ctx.getCheckpointLock();

            while (isRunning) {
                synchronized (lock) {
                    ctx.collect(offset);
                    offset += 1;
                }
            }

        }

        @Override
        public void cancel() {
            isRunning = false;

        }


    }

}
