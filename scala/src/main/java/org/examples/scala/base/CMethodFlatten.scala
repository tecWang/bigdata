package org.examples.scala.base

object CMethodFlatten {
  def main(args: Array[String]): Unit = {
    val arr = List(List(100,120,200), List(99,119,199))
    // flatten从里往外压，作用是把多维数组压成一维数组
    println(arr)
//    List(List(100, 120, 200), List(99, 119, 199))
    val arr2 = arr.flatten
    println(arr2)
//    List(100, 120, 200, 99, 119, 199)
    // 必须是多维数组的形式才可以用flatten
    // 只能压一层
    // List(("zhangsan", List(100,120,200)), ("lisi", List(99,119,199))) 这个就不可以压


  }
}
