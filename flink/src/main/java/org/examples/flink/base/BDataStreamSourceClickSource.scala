package org.examples.flink.base

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.util.Random

//class BDataStreamSourceClickSource extends SourceFunction[BDataStreamSourceBoundedEvent] {
// 上方的SourceFunction不支持并行

class BDataStreamSourceClickSource extends ParallelSourceFunction[BDataStreamSourceBoundedEvent] {
  // 创建标志位
  var runningFlag = true

  override def run(sourceContext: SourceFunction.SourceContext[BDataStreamSourceBoundedEvent]): Unit = {
    val rand = new Random()
    val users = Array("zhangsan", "lisi", "wangwu", "zhaoliu")
    val urls = Array("./cars", "./books", "./pcs", "./houses")

    // 用标志位作为循环停止条件
    while (runningFlag) {
      val event = BDataStreamSourceBoundedEvent(
        users(rand.nextInt(users.length)),
        urls(rand.nextInt(users.length)),
        Calendar.getInstance.getTimeInMillis
      )
      sourceContext.collect(event)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = runningFlag = false
}
