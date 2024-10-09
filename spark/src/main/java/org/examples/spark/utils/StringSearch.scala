package org.examples.spark.utils

import org.apache.spark.rdd.RDD

class StringSearch(override val pattern: String)
  extends Search(pattern)
  with Serializable {

  // Method 'search' overrides nothing
    // 重写成功的前提是函数的输入、输出都需要是一致的
  override def search(rdd: RDD[String]): RDD[String] = {
    rdd.filter(item => {
      item.startsWith(this.pattern)
    })
  }
}
