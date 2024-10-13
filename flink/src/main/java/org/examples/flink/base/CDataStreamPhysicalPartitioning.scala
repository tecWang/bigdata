package org.examples.flink.base

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

object CDataStreamPhysicalPartitioning {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.addSource(new BDataStreamSourceClickSource)

    // 使用shuffle打乱数据 --------------------------------------------------------------------------------------------
//    dataStream.shuffle.print("shuffle").setParallelism(4)
//    shuffle:3> BDataStreamSourceBoundedEvent(lisi,./pcs,1728783431065)
//    shuffle:3> BDataStreamSourceBoundedEvent(lisi,./pcs,1728783432081)
//    shuffle:2> BDataStreamSourceBoundedEvent(wangwu,./houses,1728783433095)
//    shuffle:1> BDataStreamSourceBoundedEvent(lisi,./books,1728783434096)
//    shuffle:3> BDataStreamSourceBoundedEvent(wangwu,./houses,1728783435107)

    // round-robin --------------------------------------------------------------------------------------------
      // 可以理解为按顺序依次发牌
      // kafka, nginx 都使用了这种方法
//    dataStream.rebalance.print("rebalance").setParallelism(4)
//    rebalance:2> BDataStreamSourceBoundedEvent(zhangsan,./houses,1728783681300)
//    rebalance:3> BDataStreamSourceBoundedEvent(wangwu,./pcs,1728783682314)
//    rebalance:4> BDataStreamSourceBoundedEvent(lisi,./cars,1728783683330)
//    rebalance:1> BDataStreamSourceBoundedEvent(lisi,./pcs,1728783684330)
//    rebalance:2> BDataStreamSourceBoundedEvent(zhangsan,./houses,1728783685344)
//    rebalance:3> BDataStreamSourceBoundedEvent(zhangsan,./books,1728783686346)
//    rebalance:4> BDataStreamSourceBoundedEvent(wangwu,./houses,1728783687365)

    // rescale 重缩放分区 --------------------------------------------------------------------------------------------
      // 跟rebalance的区别是，如果存在多个source，rebalance是分发给每个下游，而rescale会对下游分组，只对组内的下游进行轮询下发
      // 补充，哪怕只有一个source，rescale也会先分组再分发

    // 增加一个source便于测试
    val dataStream2 = env.addSource(new RichParallelSourceFunction[Int] {
      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        for (i <- 0 to 7)
          if (getRuntimeContext.getIndexOfThisSubtask == (i+1) % 2)
            sourceContext.collect(i+1)
      }

      override def cancel(): Unit = ???
    }).setParallelism(2)      // 这一步很关键，否则只能打印出来4个数字了

//    dataStream2.rescale.print("rescale").setParallelism(4)
    // 可以看到 1 3 5 7打印在了3，4子任务
//    rescale:4> 3
//    rescale:3> 1
//    rescale:1> 2
//    rescale:2> 4
//    rescale:1> 6
//    rescale:3> 5
//    rescale:4> 7
//    rescale:2> 8

    // broadcast --------------------------------------------------------------------------------------------
//    dataStream.broadcast.print("broadcast").setParallelism(2)
    // 同一条数据会往所有的下游都发送一遍
//    broadcast:1> BDataStreamSourceBoundedEvent(wangwu,./cars,1728784998300)
//    broadcast:2> BDataStreamSourceBoundedEvent(wangwu,./cars,1728784998300)
//    broadcast:1> BDataStreamSourceBoundedEvent(lisi,./pcs,1728784999307)
//    broadcast:2> BDataStreamSourceBoundedEvent(lisi,./pcs,1728784999307)
//    broadcast:2> BDataStreamSourceBoundedEvent(wangwu,./pcs,1728785000316)
//    broadcast:1> BDataStreamSourceBoundedEvent(wangwu,./pcs,1728785000316)


    // global --------------------------------------------------------------------------------------------
    // 会将所有数据全部发送到下游的第一个并行子任务中去
//    dataStream.global.print("global")setParallelism(4)
//    global:1> BDataStreamSourceBoundedEvent(zhaoliu,./cars,1728785116568)
//    global:1> BDataStreamSourceBoundedEvent(zhangsan,./cars,1728785117582)
//    global:1> BDataStreamSourceBoundedEvent(wangwu,./pcs,1728785118583)
//    global:1> BDataStreamSourceBoundedEvent(lisi,./books,1728785119584)

    // 自定义分区 --------------------------------------------------------------------------------------------
//    def partitionCustom[K: TypeInformation](partitioner: Partitioner[K], fun: T => K)
//      : DataStream[T] = {
    dataStream2.partitionCustom(new Partitioner[Int] {

      override def partition(k: Int, i: Int): Int = k % 2
    }, item => item)
      .print("partitionCustom")
      .setParallelism(4)
    // 结果按照奇偶数分区
//    partitionCustom:2> 1
//    partitionCustom:2> 3
//    partitionCustom:2> 5
//    partitionCustom:2> 7
//    partitionCustom:1> 2
//    partitionCustom:1> 4
//    partitionCustom:1> 6
//    partitionCustom:1> 8

    env.execute()
  }
}
