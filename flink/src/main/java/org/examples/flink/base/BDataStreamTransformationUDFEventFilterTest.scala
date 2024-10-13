package org.examples.flink.base

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object BDataStreamTransformationUDFEventFilterTest {
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

    // 1. 使用自定义udf
    dataStream1.filter(new BDataStreamTransformationUDFEventFilter("pc"))
      .print()
//    BDataStreamSourceBoundedEvent(wangwu,./pc,4000)
//    BDataStreamSourceBoundedEvent(zhangsan,./pc,4000)
//    BDataStreamSourceBoundedEvent(zhangsan,./pc,4000)
//    BDataStreamSourceBoundedEvent(zhangsan,./pc,4000)

    // 2. 使用匿名类
      // 匿名类无法自定义参数，只能写死了
    dataStream1.filter(new FilterFunction[BDataStreamSourceBoundedEvent] {
      override def filter(t: BDataStreamSourceBoundedEvent): Boolean = t.url.contains("pc")
    })
      .print()
    //    BDataStreamSourceBoundedEvent(wangwu,./pc,4000)
    //    BDataStreamSourceBoundedEvent(zhangsan,./pc,4000)
    //    BDataStreamSourceBoundedEvent(zhangsan,./pc,4000)
    //    BDataStreamSourceBoundedEvent(zhangsan,./pc,4000)

    // 3. 使用匿名表达式
    dataStream1.filter(item => item.url.contains("pc"))
      .print()

    env.execute()
  }
}
