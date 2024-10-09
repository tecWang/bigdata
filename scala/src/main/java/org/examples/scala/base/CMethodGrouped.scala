package org.examples.scala.base

object CMethodGrouped {
  def main(args: Array[String]): Unit = {
    // grouped 不按照条件进行分组
    // groupby 按照指定条件进行分组
    val arr = 1 until 10
    println(arr.grouped(3))   // <iterator>

    // 迭代器只能用一次
    val it = arr.grouped(3)
    it.foreach(item => println(item.toList))
//    List(1, 2, 3)
//    List(4, 5, 6)
//    List(7, 8, 9)

    val it2 = arr.grouped(2)
    println(
      it2.map(item => {
        item.reverse
      }).flatten.mkString(" ")
      // 2 1 4 3 6 5 8 7 9
    )

  }
}
