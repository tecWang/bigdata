package org.examples.scala.base

object CMethodPar {
  def main(args: Array[String]): Unit = {
    // 并行集合
//    val arr = 1 to 10000000
    val arr = 1 to 10
    arr.foreach(item => println(item))

    println("=================================================")
    arr.par.foreach(item => {
      val name = Thread.currentThread().getName
      println(name + " " + item)
    })
  }
}
