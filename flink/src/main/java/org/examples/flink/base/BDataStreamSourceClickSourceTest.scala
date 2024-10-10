package org.examples.flink.base

import org.apache.flink.streaming.api.scala._

object BDataStreamSourceClickSourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val dataStream: DataStream[BDataStreamSourceBoundedEvent] = env.addSource(new BDataStreamSourceClickSource)
    dataStream.print()
//    1> BDataStreamSourceBoundedEvent(zhangsan,./houses,1728527815060)
//    2> BDataStreamSourceBoundedEvent(lisi,./books,1728527815060)
//    1> BDataStreamSourceBoundedEvent(wangwu,./houses,1728527816069)
//    2> BDataStreamSourceBoundedEvent(zhaoliu,./pcs,1728527816069)
//    2> BDataStreamSourceBoundedEvent(zhaoliu,./books,1728527817073)

    env.execute()
  }
}
