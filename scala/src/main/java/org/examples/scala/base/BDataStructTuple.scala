package org.examples.scala.base

object BDataStructTuple {

//  元组是系统提供的共用类，用于用户在计算的过程中封装数据的，不需要用户自己去定义不同种类的类的模板
  // 元组一共定义了Tuple1 ~ Tuple22，Tuple1代表可以封装1个元素，Tuple22代表可以封装22个元素

//  元组跟数组的区别是什么？ 为什么不用数组封装对象
    // 对于数组来讲，数组中的每个元素都是单独的个体
    // 对于元组来讲，元组中所有的元素加起来才是一个完整的个体，更像是一个类。
      // 元组中的内容只能读取，不能修改

  //  定义一个元组
  val tp = (1, "zhangsan", 20)
//  scala> val tp = (1, "zhangsan", 20)
//  tp: (Int, String, Int) = (1,zhangsan,20)

  val tp2 = (1, "zhangsan", 20, Array(100, 120, 150))

  // 元组可以嵌套
  val tp3 = (1, "zhangsan", (("math", 100), ("eng", 120)))

  // swap
  val tp4 = ("zhangsan", "lisi")

  // 对偶元组
//  scala> val arr1 = Array[String]("zhangsan", "lisi")
//  arr1: Array[String] = Array(zhangsan, lisi)
//
//  scala>   val arr2 = Array[Int](120, 59)
//  arr2: Array[Int] = Array(120, 59)
//
//  scala可以非常简单的把两个数组拼接到一起
//  数组拼接1
//  scala> arr1 zip arr2
//  res2: Array[(String, Int)] = Array((zhangsan,120), (lisi,59))
//  数组拼接2
//  scala> arr1.zipWithIndex
//  res3: Array[(String, Int)] = Array((zhangsan,0), (lisi,1))

  def main(args: Array[String]): Unit = {
    println("tp select: " + tp._2)    // 靠下划线进行取值
    println("tp2 select: " + tp2._4(1))    // 120
    println("tp3 select: " + tp3._3._1._2)    // 100

    // swap select
    println("tp4 select: " + tp4._1, tp4._2)    // 100
    val tp5 = tp4.swap
    println("tp4 swaped select: " + tp5._1, tp5._2)    // 100


  }
}
