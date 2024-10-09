package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* reduceByKey v.s. groupByKey (建议优先使用前者)
*   1. reduceByKey = groupByKey + mapValues
*   2. reduceByKey 开发效率更高，只需要一段代码
*   3. reduceByKey 执行性能更高。因为在执行聚合之前，reduceByKey 会在分区内进行 combine(预聚合)，减少落盘的数据量，性能会更高
* 如何优化 shuffle？
*   1. 提升硬件性能，内存大小，磁盘 io 性能等
*   2. 增加磁盘缓冲区
*   3. 减少磁盘读写次数
* */

object BSparkRDDOperateTransformReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    // 定义元组数组
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("b", 2), ("a", 3), ("c", 4)))

    //  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
      //  def reduceByKey(func: (Int, Int) => Int) : RDD[(String, Int)]
    val rdd2: RDD[(String, Int)] = rdd.reduceByKey((a,b) => a+b)
    //    (a,4)
    //    (b,2)
    //    (c,4)

    // 输出打印预览
    rdd2.collect().foreach(println)

    //
    Thread.sleep(10000)

  }
}
