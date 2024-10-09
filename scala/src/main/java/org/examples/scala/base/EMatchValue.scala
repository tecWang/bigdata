package org.examples.scala.base

import scala.io.StdIn

// match语句类似于java中的switch，sql中的case when
object EMatchValue {
  def main(args: Array[String]): Unit = {
    while (true) {
      val num = StdIn.readInt()
//      if (num == 1) println("1")
//      else if(num == 2) println("2")
//      else println("else")

      // 值匹配
      num match {
        case 1 => println("1")
        case 2 => println("2")
        // 超出case范围之后，程序会直接报错 MatchError
        case _ => println("else")   // 守卫
      }
    }
  }
}
