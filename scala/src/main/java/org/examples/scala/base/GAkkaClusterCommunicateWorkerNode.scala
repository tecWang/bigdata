package org.examples.scala.base

import akka.actor.{Actor, ActorSystem, Props}
import akka.actor.TypedActor.dispatcher
import com.typesafe.config.ConfigFactory

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class WorkerNode(
                  val workerId:String,
                  val cores:Int,
                  val memory:String
                ) extends Actor {

//  val workerId = "worker-001"
//  val cores = 1
//  val memory = "512MB"

  def sendHeart = {
    //    initialDelay: FiniteDuration,
    //    interval:     FiniteDuration,
    //    receiver:     ActorRef,
    //    message:      Any
    // Cannot find an implicit ExecutionContext. You might pass
    // 每三秒发送一次心跳给主结点
    context.system.scheduler.schedule(
      Duration(3, TimeUnit.SECONDS),
      Duration(3, TimeUnit.SECONDS),
      sender(),
      GAkkaClusterCommunicateHeartBeat(workerId)
    )
  }

  override def receive: Receive = {
    case GAkkaClusterCommunicateRegisterSucc => {
      println("register successful ! ")
      // 开始心跳监测
      sendHeart
    }
  }

  // 初始化连接master结点
  override def preStart(): Unit = {
    // 生成一个注册都西昂
    val info = GAkkaClusterCommunicateRegisterInfo(workerId, System.currentTimeMillis(), cores, memory)
    // 连接到master节点
    val proxy = context.actorSelection("akka.tcp://nn1@localhost:8900/user/master")
//    val line = StdIn.readLine()
//    proxy ! line
    proxy ! info
  }
}

object GAkkaClusterCommunicateWorkerNode {
  def main(args: Array[String]): Unit = {
    val conf =
      """
        |akka.actor.provider = akka.remote.RemoteActorRefProvider
        |akka.remote.netty.tcp.hostname = localhost
        |akka.remote.netty.tcp.port = 8901
        |akka.actor.warn-about-java-serializer-usage = false
        |""".stripMargin
    val config = ConfigFactory.parseString(conf)
    val workerNode = ActorSystem.create("nn2", config)
      .actorOf(Props(new WorkerNode("worker-001", 1, "512MB")), "worker1")
//      .actorOf(Props(new WorkerNode("worker-002", 1, "512MB")), "worker2")
  }

  // [WARN] [SECURITY][09/03/2024 10:51:11.734] [nn2-akka.remote.default-remote-dispatcher-14] [akka.serialization.Serialization(akka://nn2)] Using the default Java serializer for class [com.learn.base.GAkkaClusterCommunicateRegisterInfo] which is not recommended because of performance implications. Use another serializer or disable this warning using the setting 'akka.actor.warn-about-java-serializer-usage'
  // 增加配置项: akka.actor.warn-about-java-serializer-usage = false
}
