package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BSparkRDDOperateTransformMapValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    // 定义元组数组
      // KV类型一般以 2元组 形式存在

    // value类型处理思路
//    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3)))
//    rdd.map(item => (item._1, item._2 * 2)).foreach(println)

    // kv类型处理思路
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3)))

    // mapValues 可以只把 kv 的 v 传进来处理
      // 方法里边可以传自己需要的自定义函数
    rdd.mapValues(item => item * 2)
      .foreach(println)
      //    (a,2)
      //    (b,4)
      //    (c,6)

  }
}
