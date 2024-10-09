package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* spark 方法 ==>
*   1. 将数据向下流转，转换算子     Transformation
*   2. 控制数据流转，行动算子。     Action
* rdd 处理数据的分类 ==>
*   1. 单值类型
*   2. 键值类型
* */

//
//
//

/*
* Exception in thread "main" java.lang.ExceptionInInitializerError at org.apache.spark.SparkContext.withScope(SparkContext.scala:701)
* stack overflow 反馈说是因为 jackson 版本兼容的问题，
*   1. 查看外部依赖，jackson 确实有两套。 注掉 mysql 模块下边的依赖后， jackson 变成一套，但是问题依然没有解决。
*   2. 进一步排查发现是，jdk 用了 openjdk22，于是调整 jdk 为 jdk8，问题解决。
*   3. 恢复mysql的依赖，jsckson 还是只有一套，暂时没有发现影响。待继续观察。
* */

object BSparkRDDOperateTransformMap {
  def main(args: Array[String]): Unit = {
    // 创建一个 conf 对象
    val conf = new SparkConf()
    conf.setMaster("local[*]")      // 指定为本地模式，否则会报错, A master URL must be set in your configuration spark
    conf.setAppName("HelloWorld")
    conf.set("spark.default.parallelism", "10")

    // 保存文件到本地
    val sc = new SparkContext(conf)

    // 从对象加载数据，并落本地文件
    val pathFromObj = "output/from_obj/"

    // parallelize 用以指定并行度
    val rdd: RDD[String] = sc.parallelize(Array("1", "2", "3", "4", "5", "6"), numSlices = 3)

    // map 的作用就是对每个元素做处理，可以拆分成 list，也可以对单个元素做处理
      // ==> A => B
        // A B可以没有任何关系
    // val value:RDD[(String, Int)] = rdd.map(item => {(item, item.toInt + 1)})
    // 默认情况下，新创建的RDD的分区数量和旧的RDD的分区数量保持一致。之前是2个，新的还是2个。

    rdd.map(item => {(item, item.toInt + 1)})
//      .collect()      // 启用 collect 之后，打印的顺序会变成有序的
      .foreach(item => println(s"item: ${item}"))
    // 遍历rdd： (乱序)
    // item: (5,6)
    //item: (1,2)
    //item: (6,7)
    //item: (3,4)
    //item: (2,3)
    //item: (4,5)

    /*
    * spark 处理数据时，
    *   分区内有序，分区间无序
    *
    * */
    println("开始验证 spark 处理顺序")
    rdd.map(item => {
      println("@" + item.toInt)
      // 分了三个分区，看起来应该是先跑每个分区的第一个元素，即 1 3 5
//        @5
//        @1
//        @3
//        @2
//        @6
//        @4
      item + 1    // 必须有这一行，否则会
    })
      .foreach(item => Unit)    //

  }
}
