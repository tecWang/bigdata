package org.examples.scala.base

object CMethodDiff {
  def main(args: Array[String]): Unit = {
    // diff       差集
    // union      并集
    // insersect  交集
    val arr1 = 1 to 10
    val arr2 = 2 to 11
    println("diff " + arr1.diff(arr2))
//    diff Vector(1)
    println("union " + arr1.union(arr2))
//    union Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    println("intersect " + arr1.intersect(arr2))
//    intersect Vector(2, 3, 4, 5, 6, 7, 8, 9, 10)
  }
}
