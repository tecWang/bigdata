package org.examples.flink.base

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object BDataStreamSourceKafka {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "consumer-group")

    // 需要利用docker创建一个kafka环境
      /*
      * 1. docker run apache/kafka
      * 2. docker run -d --name broker apache/kafka:latest
      * 3. docker exec --workdir /opt/kafka/bin/ -it broker sh
      * 4. ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic
      * 5. ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic
      *   可以在console中输入内容了，当前程序会自动接受过来。
      * */
    val dataStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer[String](
        "test-topic",
        new SimpleStringSchema(),
        props)
    )

    dataStream.print()

    env.execute()
  }
}
