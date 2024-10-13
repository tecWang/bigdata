package org.examples.flink.base

import org.apache.flink.streaming.api.scala._

object BDataStreamTransformationRichFunctionTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream1: DataStream[BDataStreamSourceBoundedEvent] = env.fromElements(
      BDataStreamSourceBoundedEvent("zhangsan", "./home", 1000L),
      BDataStreamSourceBoundedEvent("lisi", "./cart", 2000L),
      BDataStreamSourceBoundedEvent("wangwu", "./pc", 4000L),
      BDataStreamSourceBoundedEvent("zhangsan", "./pc", 4000L),
      BDataStreamSourceBoundedEvent("lisi", "./cart", 2000L),
      BDataStreamSourceBoundedEvent("lisi", "./cart", 2000L),
      BDataStreamSourceBoundedEvent("zhangsan", "./pc", 4000L),
      BDataStreamSourceBoundedEvent("zhangsan", "./pc", 4000L),
    )

    dataStream1.map(new BDataStreamTransformationRichFunction)
      .print()

    env.execute()
//    当前索引号为： 0 的任务开始执行。
//    1000
//    2000
//    4000
//    4000
//    2000
//    2000
//    4000
//    4000
//    当前索引号为： 0 的任务执行结束。
  }
}
