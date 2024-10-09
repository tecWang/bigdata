package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.examples.spark.utils.{ActorPerson, StringSearch}

object BSparkRDDOperateActionOther {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    // 定义元组数组
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10), numSlices = 2)

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

    // 执行完逻辑后的结果的数量
    val len = rdd2.count()    // 2
    println(s"len: ${len}")

    val stringToLong = rdd2.countByKey()
    println(s"stringToLong: ${stringToLong}")   // stringToLong: Map(even -> 1, odd -> 1)

    //   def first(): T = withScope {
    //    take(1) match {
    val first: (String, Int) = rdd2.first()
    println(s"first: ${first}")   // first: (even,40)

    val res: Array[(String, Int)] = rdd2.take(1)
    println(s"res: ${res(0)}")    // res: (even,40)

    // 只有 rdd 才有 save 方法，take first 等的返回值没有
    // 如果 是一个 new 的对象，保存文本就不太合适了，建议保存为对象
//    rdd.saveAsTextFile("outputFile")
//    rdd.saveAsObjectFile("outputObj")

    // 为什么要加 collect？
      // 为了保证打印结果的有序性
    rdd.collect().foreach(item => println(item))

    // 打印肯定是无序的
    rdd.foreach(item => println(s"foreach print: ${item}"))

    // 看起来打印也是无序的，重分区到1个分区可以保证数肯定是有序的
      // foreachPartition
    rdd
      .repartition(1)   // 重分区到1个分区可以保证数肯定是有序的，
      .foreachPartition(it => {
      while (it.hasNext) println(s"foreach partition print: ${it.next()}")
    })

    // -------------------------------------------------------
    //
    val rdd3 = sc.parallelize(Array(1,2,3,4), numSlices = 2)
    val a1 = new ActorPerson(age = 30, name = "zhang san")
    //    a1.sayHi()    // override by UtilAction. i am zhang san, my age is 30

    rdd3.foreach(item => {
      // Exception in thread "main" org.apache.spark.SparkException: Task not serializable
        // Caused by: java.io.NotSerializableException: base.utils.UtilActorPerson
          // object not serializable (class: base.utils.UtilActorPerson, value: base.utils.UtilActorPerson@16cb9989)
        // 原因是a1对象是在driver端创建的。但是item是在executor端存储的。如果需要把a1发送给executor，就需要先转化为字节才能传输，网络中不能直接传递对象。
        // 而a1对象因为没有序列化方法，所以就会报错
        // 另外需要注意的是，给UtilActorPerson 实现序列化之后，其父类 UtilPerson 也需要实现序列化，否则会报错。
          // scala base.utils.UtilActorPerson; no valid constructor
      println(a1.age + item)
      // 31 32
      // 33 34
    })

    // -------------------------------------------------------
    // 自定义一个工具类，并返回一个新的rdd
    val rdd4 = sc.parallelize(Array("hadoop", "spark", "scala", "java"))
    val stringSearch = new StringSearch("s")
    val rdd5: RDD[String] = stringSearch.search(rdd4)
    rdd5.collect().foreach(println)   // spark scala

    Thread.sleep(500000000)
  }
}
