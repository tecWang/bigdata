package org.examples.scala.base

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class Siri extends Actor{
  override def receive: Receive = {
    // ! 表示发送消息
    case "hello" => sender() ! "hello"
    case "you are a good person" => sender() ! "thank you"
    case _ => sender() ! "unknown msg type !"
  }
}

object GAkkaMultiPortCommunicateSiri {
  def main(args: Array[String]): Unit = {
    val conf =
      """
        |akka.actor.provider = akka.remote.RemoteActorRefProvider
        |akka.remote.netty.tcp.hostname = localhost
        |akka.remote.netty.tcp.port = 8888
        |""".stripMargin
    val config = ConfigFactory.parseString(conf)
    val iphoneActor = ActorSystem.create("iphone14", config)
    // 角色可以接收数据
    val siri = iphoneActor.actorOf(Props(new Siri), "siri")
  }
}
