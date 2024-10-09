package org.examples.scala.base

object CMethodGroupBy {
  def main(args: Array[String]): Unit = {
    val arr = 1 to 10
    // 完成数据分拣
    println(arr.groupBy(item => if (item % 2 == 0) "even" else "odd"))
//    Map(odd -> Vector(1, 3, 5, 7, 9), even -> Vector(2, 4, 6, 8, 10))

    val arr2 = List("java", "python", "c++", "js", "go", "scala")
    println(arr2.groupBy(item => item.substring(0, 1)))
//    Map(s -> List(scala), j -> List(java, js), g -> List(go), p -> List(python), c -> List(c++))

    val arr3 = List(("zhangsan", 100), ("lisi", 120), ("wangwu", 50))
    println(arr3.groupBy(item => if (item._2 >= 60) "good" else "not good"))
  }
}
