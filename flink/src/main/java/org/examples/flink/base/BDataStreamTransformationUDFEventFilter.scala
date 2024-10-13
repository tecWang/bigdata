package org.examples.flink.base

import org.apache.flink.api.common.functions.FilterFunction

// 筛选url中包含某个关键字的Event事件
class BDataStreamTransformationUDFEventFilter(val keyword:String) extends FilterFunction[BDataStreamSourceBoundedEvent]{

  override def filter(t: BDataStreamSourceBoundedEvent): Boolean = {
    t.url.contains(keyword)
  }
}
