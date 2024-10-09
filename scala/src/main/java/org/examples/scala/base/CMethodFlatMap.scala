package org.examples.scala.base

object CMethodFlatMap {
  def main(args: Array[String]): Unit = {
    val arr = List(1,2,3,4)
    val arr1 = List(arr, arr)
    println(arr1)
    // flatmap = arr.flatten.map()
    val arr2 = arr1.flatten.map(item => item*2)
    println("arr2: " + arr2)

    val arr3 = List("1 2 3 4 5", "a b c d e")
    val arr4 = arr3.map(str => str.split(" "))
      .flatten
    println(arr4)

    val arr5 = List(("a", "b"), ("c", "d"))
//    println(arr5.flatten)
//    No implicit view available from (String, String) => scala.collection.GenTraversableOnce[B].
    val arr6 = arr5.map(tp => List(tp._1, tp._2))
      .flatten
    println(arr6)
//    List(a, b, c, d)

    // flatMap = map + flattern
    // 在下边的场景里边，item就是数组里边的元组
    val arr7 = arr5.flatMap(item => Array(item._1, item._2))
    println(arr7)
//    List(a, b, c, d)
  }
}
