package org.examples.flink.base

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

/*
* 富函数有生命周期的概念
*   1. 有open close方法
*   2. 可以获取更多的上下文信息，如当前的进程号
* */
class BDataStreamTransformationRichFunction extends RichMapFunction[BDataStreamSourceBoundedEvent, Long]{

  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
    println(s"当前索引号为： ${getRuntimeContext.getIndexOfThisSubtask} 的任务开始执行。")
  }

  override def map(in: BDataStreamSourceBoundedEvent): Long = in.timeStamp

  override def close(): Unit = {
//    super.close()
    println(s"当前索引号为： ${getRuntimeContext.getIndexOfThisSubtask} 的任务执行结束。")
  }
}
