package org.examples.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BSparkRDDOperateTransformWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("spark")

    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/word.csv")

    rdd
      // 用 map 方法的话，返回值会是一堆数组， 此处应该用 flatMap
      .flatMap(item => item.split(","))
      .groupBy(item => item)
          //    (ultimate,CompactBuffer(ultimate, ultimate, ultimate, ultimate, ultimate))
          //    (tec,CompactBuffer(tec, tec, tec, tec, tec))
          //    (idea,CompactBuffer(idea, idea, idea, idea, idea))
          //    (learn,CompactBuffer(learn, learn, learn, learn, learn))
          //    (try,CompactBuffer(try, try, try, try, try))
          //    (tom,CompactBuffer(tom, tom, tom, tom, tom))
          //    (jack,CompactBuffer(jack, jack, jack, jack, jack))
          //    (hello,CompactBuffer(hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello, hello))
          //    (method,CompactBuffer(method, method, method, method, method))
          //    (csv,CompactBuffer(csv, csv, csv, csv, csv))
          //    (install,CompactBuffer(install, install, install, install, install))
      // string的 rdd 有 mapValues方法
      .mapValues(it => {
            var len = 0
            for(s <- it) len += 1
            len
          })
          //    (ultimate,5)
          //    (tec,5)
          //    (idea,5)
          //    (learn,5)
          //    (tom,5)
          //    (try,5)
          //    (hello,20)
          //    (jack,5)
          //    (csv,5)
          //    (method,5)
          //    (install,5)
      .sortByKey(ascending = false, numPartitions = 5)
          //    (ultimate,5)
          //    (try,5)
          //    (tom,5)
          //    (tec,5)
          //    (method,5)
          //    (learn,5)
          //    (jack,5)
          //    (install,5)
          //    (idea,5)
          //    (hello,20)
          //    (csv,5)
  .collect()    // 为了保证最终打印的内容有序
  .foreach(println)

  }
}
