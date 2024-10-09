package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BSparkRDDOperateTransformDistinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,1,2,3,4))

    // hashset 是单点去重，指在当前内存内去重
    // distinct 是分布式去重
      // 底层逻辑是 先分组 再去重

    rdd.distinct()
      .collect()
      .foreach(println)
    // 1 2 3 4 5
  }
}
