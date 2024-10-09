package org.examples.scala.base

object CMethodSort {
  def main(args: Array[String]): Unit = {
    val arr = List(1,9,2,8,3,7,4,6,5)

    // sorted
    // sortBy
    // sortWith
    println("arr.sorted: " + arr.sorted)
    println("arr.sorted.reverse: " + arr.sorted.reverse)
    println("arr sort by: " + arr.sortBy(item => -item))

    val arr2 = List(("zhangsan", 28000), ("lisi", 31000), ("wangwu", 1500))
    println(arr2.sortBy(item => item._2))
    //    List((wangwu,1500), (zhangsan,28000), (lisi,31000))
    println(arr2.sorted)
    //    List((lisi,31000), (wangwu,1500), (zhangsan,28000))

    val arr3 = List(List(1,2,3), List(1,1,1))
    println(arr3.sortBy(item => item.sum / item.size))

    // sortwith
    println(arr3.sortWith((a,b) => a.sum/a.length > b.sum/b.length))
  }
}
