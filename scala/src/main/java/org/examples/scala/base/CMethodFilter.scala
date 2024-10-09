package org.examples.scala.base

object CMethodFilter {
  def main(args: Array[String]): Unit = {
    val arr = List(1,2,3,4,5,6,7,8,9)
    println(arr)

    // filter处理完后元素类型不变，会生成一个新的集合，元素数量会有变化
    val arr2 = arr.filter(item => item > 5)
    println(arr2)


  }
}
