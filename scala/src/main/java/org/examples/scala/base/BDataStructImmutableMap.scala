package org.examples.scala.base

import scala.collection.mutable.Map

object BDataStructImmutableMap {

  val m = Map("zhangsan"->10, "lisi"->20)

  def main(args: Array[String]): Unit = {
    println(m)
    m += ("zhaosi"->30)
    println(m)

    // map遍历
    for (tp <- m) {
      println("-------------------------")
      println(tp)
      println(tp._1 + " " + tp._2)
    }

    // map遍历2
    for (key <- m.keys) {
      println(key + ": " + m(key))
    }

    // map遍历3
    for (key <- m.keysIterator) {
      println(key + ": " + m(key))
    }

    // map遍历4
    for (v <- m.values) {
      println(v)
    }

    // map遍历5
    for ( (k,v) <- m) {
      println(k + " " + v)
    }

    // map遍历6
    // _ 只能占位，不能打印
    for ( (k, _) <- m) {
      println(k)
    }
  }
}
