package org.examples.flink.base

//import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
// 直接引入所有内容
import org.apache.flink.api.scala._

// 本样例主要是为了展示 flink 的批处理能力
// 但是 dataset 其实已经属于过期的方法了，需要把所有数据收集齐，才能开始处理，作为有界流处理就好了
    // 建议使用 dataStream

object AFlinkWordCountDataSet {
    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment

        val lineDS:DataSet[String] = env.readTextFile("data/word.csv")
        println("lineDS", lineDS)

        // 语法会飘红，需要引入 createTypeInformation
        val lineDSplited = lineDS.flatMap(item => {
            val words = item.split(",")
//            for (word <- words) println(word)
            words
        })

        val lineDSToMap: DataSet[(String, Int)] = lineDSplited.map(item => (item, 1))

        val lineDSGroupBy: GroupedDataSet[(String, Int)] = lineDSToMap.groupBy(fields = 0)

        val lineDSumBy: AggregateDataSet[(String, Int)] = lineDSGroupBy.sum(field = 1)

        lineDSumBy.print()
        // 调用 print 方法时，一直在报错。
            // No ExecutorFactory found to execute the application in Flink
        // 查询多处均表示是缺少 flink-clients的原因，但是反复调整 pom 无果
            // 于是，从网上下载 flink-clients 并配置项目依赖，就可以正常运行了。
            // jar包下载地址: https://developer.aliyun.com/mvn/search

//        env.execute()

    }
}
