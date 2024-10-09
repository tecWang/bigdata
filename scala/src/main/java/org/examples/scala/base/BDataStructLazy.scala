package org.examples.scala.base

object BDataStructLazy {

  def step2() = {
    println(2)
    2
  }

  def main(args: Array[String]): Unit = {
//    val num = step2()
    lazy val num = step2()
    // 如果不加lazy，打印的顺序为 2 3 2 2
    // 加上之后，打印顺序为 3 2 2 2
    println(3)
    println(num)
    println(num)
  }
}
