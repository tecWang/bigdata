package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BSparkRDDOperateTransformGroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    // 定义元组数组
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("b", 2), ("a", 3), ("c", 4)))

    // groupByKey 与 groupBy 相比，其实只是节省了传key的过程，其他没啥区别

    //    def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = self.withScope {
    val rdd2:RDD[(String, Iterable[Int])] = rdd.groupByKey(numPartitions = 5)
      //    (a,CompactBuffer(1, 3))
      //    (b,CompactBuffer(2))
      //    (c,CompactBuffer(4))
        //  private[spark] class CompactBuffer[T: ClassTag] extends Seq[T] with Serializable {

    // 将 value 值相加
    // 第一种实现方式，直接 for 循环
//    val rdd3: RDD[(String, Int)] = rdd2.mapValues(it => {
//        var res = 0
//        for (value <- it) res += value
//        res
//      })
    //    (a,4)
    //    (b,2)
    //    (c,4)

    // 第二种实现方式，借助于迭代器
    val rdd3: RDD[(String, Int)] = rdd2.mapValues(it => {
      var sum = 0
      val iterator: Iterator[Int] = it.iterator
      while (iterator.hasNext)
        sum += iterator.next()
      sum
    })
    //    (a,4)
    //    (b,2)
    //    (c,4)

    rdd3
      .collect()
      .foreach(println)



  }
}
