package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BSparkRDDOperateTransformSortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,1,2,3,4))

//    def sortBy[K](f: T => K, ascending: Boolean = …, numPartitions: Int = …)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
    rdd.sortBy(
        item => item,
        ascending = true,
        numPartitions = 5)
//      .saveAsTextFile("output")   // 可以看到生成了5个文件
      .collect()
      .foreach(println)
    // 1 1 2 2 3 3 4 4 5

//    Thread.sleep(1000000)
  }
}
