package org.examples.scala.base

object CMethodAggregate {
  def main(args: Array[String]): Unit = {
    val arr = 1 to 3
//    arr.aggregate(初始化值)(局部聚合逻辑， 全局聚合逻辑)
//    局部聚合逻辑中允许参数类型不同
    // aggregate自带并行逻辑
    println(
      arr.par.aggregate(0)(
//      arr.aggregate(0)(
      (a,b) => {
        println("parial aggr: " + Thread.currentThread().getName + (a,b,a+b))
        a+b
      },
      // 对于使用par方法的时候，全局聚合函数可以直接写null，不影响函数执行
      (a,b) => {
        println("global aggr: " + Thread.currentThread().getName + (a,b,a+b))
        a+b
      }
    ))

    val arr2 = List(("a", 10), ("b", 20), ("c", 30))
    // a会被赋值为初始化值
    println(arr2.aggregate(0)(
      (a,b) => a + b._2,
      (a,b) => a + b))
  }
}
