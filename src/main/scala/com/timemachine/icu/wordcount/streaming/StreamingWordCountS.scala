package com.timemachine.icu.wordcount.streaming

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingWordCountS {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env
      .socketTextStream("localhost", 8888)
      .flatMap(line => line.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .sum(1)
      .print()

    env.execute("scala com.timemachine.icu.wordcount.streaming word count")

  }

}
