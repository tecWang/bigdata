package org.examples.flink.base

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

class BDataStreamTransformationReduceFunction extends ReduceFunction[(String, Int)] {

  override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = (t._1, t._2 + t1._2)
}

object BDataStreamTransformationReduce {

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

    // 提取当前最活跃用户
    dataStream1.map(item => (item.user, 1))
      .keyBy(item => item._1)
      .reduce(new BDataStreamTransformationReduceFunction)
      .keyBy(item => true)
      // minBy, maxBy等方法需要跟在keyBy之后，因为其返回的是一个KeyedStream而不是DataStream
//      .maxBy(1)
      // 除了用maxBy实现，也可以自定义reduce实现
        // 可以理解为用reduce实现一个逐次比大小的操作，不涉及到累加、累减的计算
      .reduce((state, data) => if (state._2 > data._2) state else data)
      .print()
    // 由于flink是流处理，所以没出现一条数据，就会打印一条日志，打印的其实是时点的最活跃用户
//    (zhangsan,1)
//    (zhangsan,1)
//    (zhangsan,1)
//    (zhangsan,2)
//    (zhangsan,2)
//    (lisi,3)
//    (lisi,3)
//    (zhangsan,4)
    env.execute()
  }
}
