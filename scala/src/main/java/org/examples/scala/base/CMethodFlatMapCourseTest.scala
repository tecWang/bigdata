package org.examples.scala.base

object CMethodFlatMapCourseTest {
  def main(args: Array[String]): Unit = {
    val arr = List("zhangsan 60 90 100 120", "lisi 100 90 98 76 59")
//    目标输出:
//    zhangsan 60
//    zhangsan 90
//      ...

    val arr2 =
      arr.map(item => item.split(" ").toList)
        .map(arr => arr.tail.map(t => List(t, arr.head)))
        .flatten
    println(arr2)
  }
}
