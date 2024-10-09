package org.examples.scala.base

import scala.io.Source

object CMethodWordCount {

  def main(args: Array[String]): Unit = {

    // V1
    val arr = List("hello tom hello jack", "hello world hello tom jack is fat")
    val arr2 = arr.flatMap(item => item.split(" "))
      .groupBy(item => item)
      .mapValues(item => item.size)
      .foreach(println)

    // V2
    println("----------------------------------------------------------------")
    val it5:Iterator[String] = Source.fromFile("data/word.csv").getLines()
    val it6:Iterator[String] = it5.flatMap(item => item.split(","))
    it6.map(item => (item, 1))
      .toList
      .groupBy(item => item._1)
//      .mapValues(item => item.size)
//      .mapValues(item => item.map(e => e._2).sum)
//      .mapValues(item => item.reduce((a,b) => ("", a._2 + b._2))._2)
//      必须用foldLeft，fold不可以，fold要求输入必须一致？
//      .mapValues(item => item.foldLeft(0)((a,b) => a + b._2))
      .mapValues(item => item.aggregate(0)((a,b) => a + b._2, null))
      .foreach(println)

    // V3 使用下划线开始改造
    println("----------------------------------------------------------------")
    val it7:Iterator[String] = Source.fromFile("data/word.csv").getLines()
    val it8:Iterator[String] = it7.flatMap(_.split(","))
    it8.map((_, 1))
      .toList   // 迭代器不能分组，需要先转化为list
      .groupBy(_._1)
      .mapValues(_.size)
      .foreach(println)

  }
}
