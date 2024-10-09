package org.examples.scala.base

object EMatchComplex {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,4)
    arr match {
      case x:Array[Int] => println("int array")
      case Array(1,2,3,4) => println("exact array for array(1,2,3,4)")
      case Array(a,b,_,d) => println(s"a: ${a}, b: ${b}, d: ${d}")
      case _ => println("else")
    }
  }
}
