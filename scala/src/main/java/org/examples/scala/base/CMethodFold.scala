package org.examples.scala.base

object CMethodFold {
  def main(args: Array[String]): Unit = {
    val arr = List(1,2,3,4,5)
//    scala> arr.fold
//    def fold[A1 >: Int](z: A1)(op: (A1, A1) => A1): A1
    // 多了一个初始化值
    // 要求输入，输出的泛型一致
    println(arr.fold(0)((a,b) => a+b))

    // 假如需要遍历的元素是元组，就需要初始值，输出等都是元组才可以，使用场景比较受限
    val arr2 = List(("a", 10), ("b", 20), ("c", 30))
    println(arr2.fold(("", 0))((a,b) => ("", a._2 + b._2)))

//    ================================================================
    // foldLeft foldRight
    // 跟reduceleft，reduceright没啥区别
  }
}
