package org.examples.scala.base

object CMethodForEach {
  def main(args: Array[String]): Unit = {

    // 一维数据遍历
    // foreach因为没有返回值，所以一般是作为整个处理过程的尾部处理节点，map通常作为中间处理节点
    val arr = Array(1,2,3,4,5,6,7)
    arr.foreach((item) => {   // 此处是一个匿名函数
      println(item)
    })

    // 二维数组遍历
    val arr2 = Array(Array(11,22,33), Array(4,5,6))
    arr2.foreach(sub_arr => {
      sub_arr.foreach(item => {
        println(item)
      })
    })


  }
}
