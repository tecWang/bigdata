
package org.examples.spark.base

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.io.File
import java.nio.file.{Files, Paths}

// 借助于 spark 的分片处理能力，将文件拆分成多份，并保存到本地
object ASparkSaveAsTextFileMultiPartition {

  // 递归删除目录及文件
  def delDirAndFiles(path: File) {
    if (!path.exists())
      println("path is not exists")
    else if (path.isFile) {
      path.delete()
      println(s"${path} is being deleted !")
    }else if (path.isDirectory) {
      val ps: Array[File] = path.listFiles()
      for (p <- ps) delDirAndFiles(p)
      path.delete()   // 删除目录
    }
  }

  // 保存文件，如果已存在，则删除后重新保存
  def saveFile(value:RDD[String], path:String){

    if (Files.exists(Paths.get(path))) {
      // 遍历并删除下边所有文件
      delDirAndFiles(new File(path))
      // 保存文件
      value.saveAsTextFile(path)
    }
    else {
      // 直接保存文件
      value.saveAsTextFile(path)
    }
  }

  def main(args: Array[String]): Unit = {

    // 创建一个 conf 对象
    val conf = new SparkConf()
    conf.setMaster("local[*]")      // 指定为本地模式，否则会报错, A master URL must be set in your configuration spark
    conf.setAppName("HelloWorld")
    conf.set("spark.default.parallelism", "10")

    // 保存文件到本地
    // HADOOP_HOME and JAVA_HOME Not Set Correctly on Windows
    // 解决办法： 搞到 hadoop wintuils二进制文件，配置环境变量。参考：https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems
    val sc = new SparkContext(conf)

    // 从对象加载数据，并落本地文件
    // Idea中默认得根路径是项目路径，而不是模块根路径
    val pathFromObj = "output/from_obj/"
    // 分片参数以此处的数量为准，会覆盖初始化的 local[*] 的参数，正常应该会拆分成 7 份
    // 翻源码发现以下逻辑，优先取 spark.default.parallelism（默认并行度）， 如果不存在，以 totalCores（当前环境总虚拟核数） 为准。
    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //      jsc.parallelize(names, numSlices = 5).saveAsTextFile(path)    // 此处的5优先级更高
    // parallelize 用以指定并行度
    val value = sc.parallelize(Array("1","2","3","4","5","6"), numSlices = 3)
    saveFile(value, pathFromObj)
    /*
    * 存储结果：
    * input: 1, 2, 3, 4
    * part-00000: 1
    * part-00001: 2
    * part-00002: 3,4
    *
    * input: 1, 2, 3, 4, 5
    * part-00000: 1
    * part-00001: 2,3
    * part-00002: 4,5
    *
    * input: 1, 2, 3, 4, 5, 6
    * part-00000: 1,2
    * part-00001: 3,4
    * part-00002: 5,6
    *
    * 创建一个切分索引，为了便于做数据的切分
    *  def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
    *     (0 until numSlices).iterator.map { i =>
    *         val start = ((i * length) / numSlices).toInt
    *         val end = (((i + 1) * length) / numSlices).toInt
    *         (start, end)
    *   }
    * }
    * */

    // 从文件夹杂及数据，并落本地文件
    val pathFromFile = "output/from_file/"
      // def textFile(path: String, minPartitions: Int = defaultMinPartitions)
        // math.min(defaultParallelism, 2)
        /*
        * spark 读取文件 本质上是调用 hadoop 实现的，文件得切片数量也不是由 spark 决定得，而是由 hadoop 决定得。
        * hadoop 切片规则，
        *   total  size: 7 bytes
        *   target size: 7 / minPartitions(2) byte = 3 bytes
        *   num of partitions: 7 / 3 = 2 .... 1 partitions => 3 partitions
        *
        * spark 不支持文件操作，都是依赖于 hadoop 的
        *   1. 分区数量计算的时候，考虑的是尽可能的平均，按照字节来计算
        *   2. 分区数据的存储，考虑的是业务的完整性，按照行来读取。并且要考虑偏移量，偏移量从 0 计算。
        * 1CRLF = 3 bytes
        * 2CRLF = 3 bytes
        * 3 = 1 byte
        * 下边的[0,3]就是偏移量，hadoop就是按照该偏移量计算存储内容的
        * 【3】 == [0, 3] == 1LF2LF (注意两边都是闭区间，其实是 0 1 2 3 四个位置，所以可以放得下1和2)
        * 【3】 == [3, 6] == 3LF
        * 【1】 == [6, 7] ==
        *
        * --------------------------------------------------------------
        * case 2
        *
        * 11@@
        * 22@@
        * 33@@
        * 44
        *
        * totalSize = 14 bytes
        * minPartitions = 4
        * targetSize = 14 / 4 = 3 bytes
        * numPartitions = 14 / 3 = 4 ... 2 ==> 5 partitions
        *   分区的规则是既定的，只要文件的 totalSize 和 minPartitions 确定了，最终的分区数量就确定了，不会改。
        *   但是内容的读取规则是按照行来优先读取的，即，假如文件只有一行，但是算出来五个分区，那后四个分区都是空的，会造成数据倾斜
        * posi range:
        *   [0,3]
        *         ==> aa@b    (按照偏移量的计算规则，其实应该取到 aa@b，但是hadoop优先按行读，所以读到 aa@ )
        *         ==> aa@
        *   [3,6] ==> bb@
        *   [7,10]  ==> cc@
        *   [10,13] ==> dd@
        *   [13,14] ==>
        * */
    val value2 = sc.textFile("data/demo.txt", minPartitions = 4)
    saveFile(value2, pathFromFile)
  }

}
