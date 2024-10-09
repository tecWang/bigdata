package org.examples.scala.base

object CMethodMapValues {
  def main(args: Array[String]): Unit = {
    val arr = 1 to 10
    val arr2 = arr.groupBy(item => if(item % 2 == 0) "even" else "odd")
    println(arr2)
    //    Map(odd -> Vector(1, 3, 5, 7, 9), even -> Vector(2, 4, 6, 8, 10))
    val res = arr2.map(t => (t._1, t._2.sum / t._2.size * 1.0))
    println(res)
    //    Map(odd -> 5.0, even -> 6.0)

    // wordcount demo
    val arr3 = List("java", "python", "c++", "js", "go", "scala", "go", "python")
    val arr4 = arr3.groupBy(t => t)
    println(arr4)
    val res2 = arr4.map(item => (item._1, item._2.size))
    // mapvalues的输入必须是map集合，比如groupby的输出，或者自己生成的map
    val res4 = arr4.mapValues(item => item.size)
    println(res2, res4)

    val map = Map(("zhangsan", 10000), ("lisi", 12000), ("wangwu", 30000))
    val map2 = map.map(item => (item._1, item._2 + 1000))
    val map3 = map.mapValues(t => t+1000)
    println(map2, map3)
//    (Map(zhangsan -> 11000, lisi -> 13000, wangwu -> 31000),Map(zhangsan -> 11000, lisi -> 13000, wangwu -> 31000))
  }
}
