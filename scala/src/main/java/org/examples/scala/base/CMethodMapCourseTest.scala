package org.examples.scala.base

object CMethodMapCourseTest {
  def main(args: Array[String]): Unit = {
    val arr = List(("zhangsan", List(100,120,200)), ("lisi", List(99,119,199)))
    println(arr)

    // 求每个人的最高成绩
    val arr2 = arr.map(tp => (tp._1, tp._2.max))
    println(arr2)
//    List((zhangsan,List(100, 120, 200)), (lisi,List(99, 119, 199)))
//    List((zhangsan,200), (lisi,199))

    // 求每个人大于100分的课程
    val arr3 = arr.map(
      tp => (
        tp._1,
        tp._2.map(score => if (score > 100) 1 else 0)
          .sum
      )
    )
    println(arr3)

    // 解法2
    val arr4 = arr.map(
      tp => (
        tp._1,    // 姓名
        tp._2.filter(item => item > 100)
          .size
      )
    )
    println(arr4)
  }
}
