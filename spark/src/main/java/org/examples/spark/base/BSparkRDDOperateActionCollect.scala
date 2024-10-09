package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BSparkRDDOperateActionCollect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    // 定义元组数组
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))

    //  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    //  def reduceByKey(func: (Int, Int) => Int) : RDD[(String, Int)]
    val rdd2: RDD[(String, Int)] = rdd
      // spark 编写代码时，调用转换算子，是不会真正执行的，只是在 driver 端组合功能。
        // 转换算子中的逻辑代码都是在 executor 端执行的
      // main 方法更适合称之为 driver 方法。
      .map(item => {
        println(s"item: ${item}")   // 此处的打印是打印在 executor 端的
        item + 2
      })
      .map(item => (if (item % 2 == 0) "even" else "odd", item))
      .reduceByKey((a,b) => a+b)
    //    (a,4)
    //    (b,2)
    //    (c,4)

    // sortBy也会启动一个新job，那它是不是行动算子呢？
      // sortBy返回了新的RDD，所以是转换算子
      // 不以是否触发新任务为准
      // val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
      // sortBy方法会提前采样，调用collect方法，所以会触发作业
    // def sortBy[K](f: T => K, ascending: Boolean = …, numPartitions: Int = …)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
    val rdd3: RDD[(String, Int)] = rdd2.sortBy(item => item, ascending = true, numPartitions = 100)

    // 输出打印预览
      // collect 将 executor 的执行的结果，按照分区的顺序 拉取到 driver 端
      // collect 的返回值不是 RDD，是一个具体的查询结果
      // 生产环境下，collect 方法是不建议使用的，可能会导致多个 executor 的数据同时拉取到 driver 端导致 OOM
      // 建议把数据直接保存在文件中
    //    def collect(): Array[T] = withScope {
    //      val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    //      Array.concat(results: _*)
    //    }
    rdd3.collect()
    rdd3.collect()
    rdd3.collect()
    rdd3.collect()
    rdd3.collect().foreach(println)   // (even,30), (odd,25)
    // spark web ui 可以查询到一共出现了五个job，每个job拆分了两个stage
      // map at BSparkRDDOperateActionCollect.scala:18
      // collect at BSparkRDDOperateActionCollect.scala:29

    println("calculation finished .")

    val res: Array[(String, Int)] = rdd3.collect()
    //    res: [Lscala.Tuple2;@3002e397
    println(s"res: ${res}")
    for (item <- res) {
      println(s"item: ${item}")
      //      item: (even,40)
      //      item: (odd,35)
    }

    Thread.sleep(500000000)
  }
}
