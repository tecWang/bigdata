package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 指定样例对象，可以自动实现 apply方法
case class SparkComparableActor (val age: Int, val name: String) extends Comparable[SparkComparableActor]{

  //  Params: o – the object to be compared.
  //  Returns: a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
    // 是自己跟目标比较，如果比目标小，就是-1，相等就是0，大于就是1
  override def compareTo(that: SparkComparableActor): Int = {
    // 优先比较年龄
    if (this.age < that.age) -1
    // 如果年龄相同，比较姓名
    else if (this.age == that.age) {
      if (this.name < that.name) -1
      else if (this.name == that.name) 0
      else 1
    }
    else 1
  }
}

object BSparkRDDOperateTransformSortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("b", 2), ("a", 3), ("c", 4)))

    // 排序规则
    // 不同的key肯定按照key来排序
      // 相同的key如何排序？
    val rdd2: RDD[(String, Int)] = rdd.sortByKey(ascending = false, numPartitions = 10)
      //    (c,4)
      //    (b,2)
      //    (a,1)
      //    (a,3)

      rdd2
      .collect()
      .foreach(println)

    // 如果想要按照 kv 的 v 排序怎么办？
      // 调转 key 值进行排序，再重新处理成元组
    rdd2.map(item => (item._2, (item._1, item._2)))
      .sortByKey(ascending = true, numPartitions = 10)
      .map(item => item._2)
      //    (a,1)
      //    (b,2)
      //    (a,3)
      //    (c,4)
      .collect()
      .foreach(println)

    // 高级一点的key，需要重写 Comparable 的 compareTo 方法
    val rdd3: RDD[(SparkComparableActor, Int)] = sc.parallelize(Array(
      (new SparkComparableActor(20, "zhang san"), 1),
      (new SparkComparableActor(18, "li si"), 2),
      (new SparkComparableActor(15, "wang wu"), 3),
      (new SparkComparableActor(18, "zhao liu"), 4),
      (new SparkComparableActor(18, "wang liu"), 5)
    ))

    val rdd4: RDD[(SparkComparableActor, Int)] = rdd3.sortByKey(ascending = true, numPartitions = 10)
//    (SparkComparableActor(15,wang wu),3)
//    (SparkComparableActor(18,li si),2)
//    (SparkComparableActor(18,wang liu),5)
//    (SparkComparableActor(18,zhao liu),4)
//    (SparkComparableActor(20,zhang san),1)

    rdd4.collect().foreach(println)
  }
}
