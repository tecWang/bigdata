package org.examples.scala.base

trait MagicFunctions {
  def fire = println("i can fire")
}

class Child {}

object DObjectAbstractClassDemo2 {
  def main(args: Array[String]): Unit = {
    // scala可以在new的时候直接使用接口特性，而不是非得在定义class的时候实现
    val c = new Child with MagicFunctions
    c.fire
  }
}

