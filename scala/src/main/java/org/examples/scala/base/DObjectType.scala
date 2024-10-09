package org.examples.scala.base

object Mouse

class Mouse

object DObjectType {
  def main(args: Array[String]): Unit = {

//    val m1: Mouse.type = Mouse
//    val m2: Mouse = new Mouse

    // type关键字, 可以用于类型的传递
    type diy = (Int, String, Int, String, String)
    val tp: diy = (1, "zhangsan", 20, "beijing", "Sale")

    val l:List[Int] = List(1,2,3,4,5)
  }
}
