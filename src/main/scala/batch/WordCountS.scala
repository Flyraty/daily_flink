package batch

import org.apache.flink.api.scala._

object WordCountS {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val lines = env
      .readTextFile("/Users/zaoshu/PersonalProject/daily_flink/src/main/resources/word.txt")
      .flatMap(line => line.split(" "))
      .map(x => (x, 1))
      .groupBy(0)
      .sum(1)

    lines.print()

  }

}
