package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BSparkRDDOperateTransformGroupBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(Array("1","2","3","4","2","1","3"))

    // 字符串转整型
    val rdd2 = rdd.map(item => item.toInt)

    // 按照奇数、偶数分组

    rdd2.groupBy(item => if (item % 2 == 0) "even" else "odd")
      .collect()
      .foreach(println)
    //    (even,CompactBuffer(2, 4, 2))
    //    (odd,CompactBuffer(1, 3, 1, 3))

    // 默认 spark 数据处理时，数据所在的分区不会发生变化。
      // 但是 groupby 算子会打乱数据当前分区，并重组。
      // shuffle 操作一定会落文件，在所有文件落完之前，不允许读文件
//    rdd2.groupBy(item => item).foreach(println)
    // (2,CompactBuffer(2, 2))
    // (1,CompactBuffer(1, 1))
    // (3,CompactBuffer(3, 3))
    // (4,CompactBuffer(4))

    Thread.sleep(1000000000)

  }
}
