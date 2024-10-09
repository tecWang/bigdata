
package org.examples.spark.base

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/*
* 转换算子： 将旧的 rdd 转换为新的 rdd
*   value类型：
*     map, flatmap, groupBy
*     filter, distinct, sortBy
*     filter没有改变分区的能力，如果filter
*   key-value类型：
*     mapValues, groupByKey, reduceByKey, sortByKey
* 行动算子：
*   collect, count, countByKey
*   first, take
*   save
*   foreach, foreachPartition
* */

// 变更目录或模块后，要记得 mark source root，否则会报 找不到 xx 类
object ASparkHelloWorld {

  def main(args: Array[String]): Unit = {

    // 创建一个 conf 对象
    val conf = new SparkConf()
    conf.setMaster("local[2]")      // 指定为本地模式，否则会报错, A master URL must be set in your configuration spark
    conf.setAppName("HelloWorld")

    val spark = SparkSession.builder()
      // windows 环境运行需要指定，否则会报错。HADOOP_HOME and JAVA_HOME Not Set Correctly on Windows
      .config(conf)
      .getOrCreate()

    // 读取文件
    // 可以看到 textFile 方法的返回值 是 _root_.org.apache.spark.sql.Dataset
    // def textFile(path: _root_.scala.Predef.String): _root_.org.apache.spark.sql.Dataset[_root_.scala.Predef.String] = ???
    // 此处的textFile其实是一个就是一个 Dataset 的实例化对象
    // class Dataset[T](@DeveloperApi @Unstable val queryExecution: QueryExecution, @DeveloperApi @Unstable val encoder: Encoder[T])
    val textFile:Dataset[String] = spark.read.textFile("data/word.csv")

    // 统计 a b 两个字母的数量
    val numAs = textFile.filter(line => line.contains("a")).count()
    val numBs = textFile.filter(line => line.contains("b")).count()

    // 打印对象
    println(s"numAs: ${numAs}, numBs: ${numBs}")    // numAs: 4, numBs: 0

    // 打印
    textFile.collect().foreach(item => println(s"item: ${item}"))
//    item: hello,tom,hello,jack,tec
//    item: hello,method,idea
//    item: ultimate,csv
//    item: try,install,learn,hello

    // 休眠5秒，用来查看spark页面
    Thread.sleep(40000)

    // 停止
    spark.stop()
  }
}
