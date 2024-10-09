package org.examples.scala.base

import MyEnum.sexEnum

class Message[T] (val msg: T) {
  def getMsg():T = msg
}

class Json (val msg:String, val data:String)

object MyEnum extends Enumeration {
  // 定义枚举类型，用于泛型
  type sexEnum = Value
  // 定义码值
  val boy, girl  = Value
}

class FXDemo[A,B,C,D] (val a:A, val b:B, val c: C, val d: D) {
  def printInfo() = println(s"a = ${a}; b = ${b}; c = ${c}; d = ${d}")
}

object DObjectGenericClass {
  def main(args: Array[String]): Unit = {

    val m = new Message[String]("hello")
    println(m.getMsg())

    val m2 = new Message[Json](new Json("success", "data"))
    println(m2.getMsg().msg, m2.getMsg().data)    // (success,data)

    // 需要导入才能使用sexEnum泛型 ： import com.learn.base.MyEnum.sexEnum
    val f = new FXDemo[String, Int, Double, sexEnum]("Hello", 10, 1.1, MyEnum.boy)
    f.printInfo()   // a = Hello; b = 10; c = 1.1
  }
}
