package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BSparkRDDOperateTransformFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6))

    // filter不会打乱数据
    val rdd2: RDD[Int] = rdd.filter(item => item % 2 == 0)


    rdd2
      // 缩减、合并分区，
      // coalesce 默认没有 shuffle 功能，没有办法打乱数据重新组合，所以也没办法扩大分区，只能缩减分区
      .coalesce(1)
      // 如果想要扩大分区，需要指定 shuffle 为 true
      .coalesce(numPartitions = 10, shuffle = true)
      // 重分区, 一定有 shuffle
      // repartition 底层就是 coalesce(numPartitions, shuffle = true)
      .repartition(numPartitions = 100)
      .collect()
      .foreach(println)   // 2 4 6

  }
}
