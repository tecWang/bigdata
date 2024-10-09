package org.examples.flink

//1. 打包
//  需要打开想要package的文件作为主文件再打包，才能把文件打进去
//2. docker内部是互通的，所以监听的ip地址可以改成 ubuntu 的那台机器

// StreamExecutionEnvironment有两个位置
// 1. org.apache.flink.streaming.api.scala
// 2. org.apache.flink.streaming.api.scala.Environment
// 需要导入第一个package
import org.apache.flink.streaming.api.scala._

/*
* 1. 有界流
*   可以通过读取文本方式的方式实现，按行读取
* 2. 无界流
* */

object AFlinkWordCountDataStream {
  def main(args: Array[String]): Unit = {

    // 准备环境并读取文件
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 有界流，从文件读取数据
    // val lineDS = env.readTextFile("data/word.csv")
    // 无界流，直接从服务器接收数据
    // 此处尝试在本地利用docker创建了一个ubuntu的环境，并使用ncat命令发送数据用于监听
    // apt-get update
    // apt-get -y install ncat
    // ncat -kl 7777
    // 开始输入字符串，就可以在命令行看到信息的变化
//    val lineDS = env.socketTextStream(hostname = "localhost", port = 8080)
    val lineDS = env.socketTextStream(hostname = "172.17.0.2", port = 7777)
    //    3> (hello,3)
    //    6> (hello world,1)
    //    3> (hahaha,1)

    // 开始处理数据
    val lineDSMaped: DataStream[(String, Int)] = lineDS.flatMap(_.split(","))
      .map(item => (item, 1))

    // lineDSMaped.keyBy()
    // @deprecated("use [[DataStream. keyBy(KeySelector)]] instead")
    // def keyBy(fields: Int*): KeyedStream[T, Tuple
    lineDSMaped.keyBy(item => item._1)
      .sum(1)
      .print()

    // 流处理需要 execute 才会开始执行，否则所有代码都不会执行。
    env.execute()
    //    6> (idea,1)
    //    6> (idea,2)
    //    2> (tom,1)
    //    5> (learn,1)
    //    6> (idea,3)
    // idea 出现了多次：
    // stream的处理模式是每来一个单词就处理一次
    // 文件读取的顺序：
    //
    // 输出前边的数字的含义:
    // 并行执行的编号，初步看起来是按照单词的拆分，每个单词一个并行
  }
}
