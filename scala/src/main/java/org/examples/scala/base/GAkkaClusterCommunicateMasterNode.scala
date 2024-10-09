package org.examples.scala.base

import akka.actor.{Actor, ActorSystem, Props}
import akka.actor.TypedActor.dispatcher
import com.typesafe.config.ConfigFactory

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.Duration
//import scala.collection.mutable.ListBuffer

// worker结点在右上角编译选项中，勾选允许并行启动: allow multi instances
class MasterNode extends Actor {

  val CHECK_INTERVAL = 5000    // 5s = 5000ms
  val WORKER_TIMEOUT = 15000    // 15s无法连接认为是结点断联
  // 注册一个可变列表
  // 初始化对象的时候，后边需要跟一个括号
  //  val workerBuffer = ListBuffer[GAkkaClusterCommunicateRegisterInfo]()
  // 此处需用用var，val无法重新赋值
  var workerBuffer = mutable.Map[String, GAkkaClusterCommunicateRegisterInfo]()

  override def receive: Receive = {
    case "hello" => sender() ! "hello new worker !"
    // 如果传过来的是从节点注册信息，直接把注册类添加到buffer内
    case x:GAkkaClusterCommunicateRegisterInfo => {
      // workerBuffer.append(x)   // 优化worker存储的数据结构，改用map
      workerBuffer.put(x.workId, x)
      println(s"Log info: ${x.workId} has been added into the workerBuffer ! Current worker buffer size: ${workerBuffer.size}")
      sender() ! GAkkaClusterCommunicateRegisterSucc
    }
    // 如果传过来的是从节点心跳，启动心跳监测与反馈
    case x:GAkkaClusterCommunicateHeartBeat => {
      workerBuffer.get(x.workerId) match {
        // 如果能取到注册信息
        case Some(v) => {
          v.lastUpdTime = System.currentTimeMillis()
          workerBuffer.put(x.workerId, v)
          println(s"WorkerId: ${x.workerId} is heart beating, Current num of active workers: ${workerBuffer.size}")
        }
        case none => println(s"workerId is invalid !")
      }
    }
    // 是否要开始检测超时
    case GAkkaClusterCommunicateTimeout => {
      if(workerBuffer.size > 0) {
        workerBuffer = workerBuffer.filter(tp => {
          val workerId = tp._1
          val registerInfo = tp._2
          if(System.currentTimeMillis() - registerInfo.lastUpdTime > WORKER_TIMEOUT) {
            println(s"workId: ${workerId} is invalid")
            false
          } else {
            true
          }
        })
      }
    }
    case _ => sender() ! "else info"
  }

  // 主结点每隔3s自动检测从节点是否超时断联
  // 一旦启动就要开始监测
  override def preStart(): Unit = {
    context.system.scheduler.schedule(
      Duration(CHECK_INTERVAL, TimeUnit.MILLISECONDS),
      Duration(CHECK_INTERVAL, TimeUnit.MILLISECONDS),
      self,
      GAkkaClusterCommunicateTimeout
    )
//    WorkerId: worker-001 is heart beating, Current num of active workers: 2
//    WorkerId: worker-001 is heart beating, Current num of active workers: 2
//    workId: worker-002 is invalid
//    WorkerId: worker-001 is heart beating, Current num of active workers: 1
//    WorkerId: worker-001 is heart beating, Current num of active workers: 1
  }
}

object GAkkaClusterCommunicateMasterNode {
  def main(args: Array[String]): Unit = {
    val conf =
      """
        |akka.actor.provider = akka.remote.RemoteActorRefProvider
        |akka.remote.netty.tcp.hostname = localhost
        |akka.remote.netty.tcp.port = 8900
        |akka.actor.warn-about-java-serializer-usage = false
        |akka.log-dead-letters = 0
        |akka.log-dead-letters-during-shutdown = false
        |""".stripMargin
    val config = ConfigFactory.parseString(conf)
    val masterNode = ActorSystem.create("nn1", config)
      .actorOf(Props(new MasterNode), "master")
  }
}
