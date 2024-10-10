package org.examples.flink.base

import org.apache.flink.streaming.api.scala._

//创建一个样例类
case class BDataStreamSourceBoundedEvent (val user:String,
                                          val url:String,
                                          val timeStamp:Long)

object BDataStreamSourceBounded {
  def main(args: Array[String]): Unit = {

    // 1. 准备环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 使输出有顺序
    env.setParallelism(1)

    // 2. 从元素中读取数据
    val dataStream1: DataStream[BDataStreamSourceBoundedEvent] = env.fromElements(
      BDataStreamSourceBoundedEvent("zhangsan", "./home", 1000L),
      BDataStreamSourceBoundedEvent("lisi", "./cart", 2000L),
      BDataStreamSourceBoundedEvent("wangwu", "./pc", 4000L)
    )
    dataStream1.print("dataStream1")

    // 3. 从集合中读取数据
    val dataStream2:DataStream[BDataStreamSourceBoundedEvent] = env.fromCollection(Array(
      BDataStreamSourceBoundedEvent("zhangsan", "./home", 1000L),
      BDataStreamSourceBoundedEvent("lisi", "./cart", 2000L),
      BDataStreamSourceBoundedEvent("wangwu", "./pc", 4000L)
    ))
    dataStream2.print("dataStream2")

    env.execute()
  }
}
