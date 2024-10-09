package org.examples.scala.base

import akka.actor.{Actor, ActorSystem, Props}

/*
* spark的rpc是通过akka实现的
*
* */

class MyActor extends Actor {
  override def receive: Receive = {
    case msg => println(s"received msg: ${msg}")    // received msg: This is the first message from actor1
  }
}

object GAkka {
  def main(args: Array[String]): Unit = {
    // 进程启动之后，是不会停止的，会持续接收外部的消息
    val system1 = ActorSystem.create("system1")
    val actor1 = system1.actorOf(Props(new MyActor), name ="zhangsan")
    // 自己给自己发消息
    actor1 ! "This is the first message from actor1"

  }
}
