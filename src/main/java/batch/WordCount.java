package batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env
                .readTextFile("/Users/zaoshu/PersonalProject/daily_flink/src/main/resources/word.txt")
                .setParallelism(1);

        // 获取 source 的并行度
        System.out.println(((DataSource<String>) text).getParallelism());

        DataSet<Tuple2<String, Integer>> counts =
            text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // normalize and split the line
                        String[] tokens = value.toLowerCase().split(" ");

                        // emit the pairs
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                out.collect(new Tuple2<String, Integer>(token, 1));
                            }
                        }
                    }
                })
                .groupBy(0)
                .sum(1);

        counts.print();

    }
}
