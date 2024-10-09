package org.examples.scala.base

object BDataStructTupleCourseTest {

  private def getCount(arr:Array[Int], num:Int):(Int,Int,Int) = {

    var le_cnt = 0
    var eq_cnt = 0
    var ge_cnt = 0

    for(item <- arr) {
      if (item < num) le_cnt += 1
      else if (item > num) ge_cnt += 1
      else eq_cnt += 1
    }

    (le_cnt, eq_cnt, ge_cnt)
  }

  private val getStat = (arr:Array[Int]) => {
    (arr.max, arr.min, arr.sum * 1.0 / arr.length)
  }

  private def getString(arr:Array[Int]):String = {
    // 拆分奇偶两个数组
    val arr1 = for (idx <- arr.indices by 2) yield arr(idx)
    val arr2 = for (idx <- 1 to arr.length by 2) yield arr(idx)
    // 两个数组合并成元组数组
    val arr3 = arr2 zip arr1
    // 遍历数组
    val res = for (tp <- arr3) yield tp._1 + " " + tp._2
    // 拼接字符串
    res.mkString(" ")
  }

  def main(args: Array[String]): Unit = {
    println("getCount return: " + getCount(Array(1, 2, 3, 4, 5), 4))   // (3,1,1)
    println("getStat return: " + getStat(Array(1, 2, 3, 4, 6)))        // (6,1,3.2)

    println("getString return: " + getString(Array(1,2,3,4,5,6,7,8)))  // 2 1 4 3 6 5 8 7
  }
}
