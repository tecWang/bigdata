package org.examples.spark.utils

import org.apache.spark.rdd.RDD

abstract class Search(val pattern: String)
  extends Serializable {

  def search(rdd: RDD[String]): RDD[String]

}
