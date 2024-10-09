package org.examples.scala.base

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.io.StdIn

class TecWang extends Actor{
  override def receive: Receive = {
    case msg => {
      println(s"remote said: ${msg}")
      println("say more !")
      val line = StdIn.readLine()
      sender() ! line
    }
  }

  override def preStart(): Unit = {
    println("Start communicate with Siri ...")
    val line = StdIn.readLine()
    // 连接到siri监听的端口
    val proxy = context.actorSelection("akka.tcp://iphone14@localhost:8888/user/siri")
    proxy ! line
  }
}

object GAkkaMultiPortCommunicatePerson {
  def main(args: Array[String]): Unit = {
    val conf =
      """
        |akka.actor.provider = akka.remote.RemoteActorRefProvider
        |akka.remote.netty.tcp.hostname = localhost
        |akka.remote.netty.tcp.port = 8889
        |""".stripMargin
    val config = ConfigFactory.parseString(conf)
    val actor = ActorSystem.create("asia", config)
    val tecWang = actor.actorOf(Props(new TecWang), "tecWang")
    // 先启动siri的进程，再启动person的进程
    // 然后就可以再命令行开始对话了，以下为对话记录
//    Start communicate with Siri ...
//    hello
//    remote said: hello
//    say more !
//      hello world
//      remote said: unknown msg type !
//    say more !
//      you are a good person
//    remote said: thank you
//      say more !
//    ha ha ha
//    remote said: unknown msg type !
//    say more !
  }
}
