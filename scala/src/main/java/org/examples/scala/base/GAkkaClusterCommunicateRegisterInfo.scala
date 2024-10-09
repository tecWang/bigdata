package org.examples.scala.base

// 注册为 case class
case class GAkkaClusterCommunicateRegisterInfo(
                                                val workId:String,
                                                var lastUpdTime:Long,
                                                val cores:Int,
                                                val memory:String
                                              ) {

}

case class GAkkaClusterCommunicateRegisterSucc() {}

case class GAkkaClusterCommunicateHeartBeat(
                                           val workerId:String
                                           ) {}

case class GAkkaClusterCommunicateTimeout() {}