package org.examples.scala.base

object BDataStructMutableMap {

  // -> 其实是一个方法，用来生成对偶元组
  // zhangsan.->(20) 含义是一样的
  // “zhangsan”->20 == ("zhangsan",20)    // true
  val m1 = Map[String, Int]("zhangsan"->20, "lisi"->30)
  val m2 = Map("zhangsan"->20, ("lisi",30))

  // 取值的时候一定存在的时候才能直接取，否则建议结合contains使用。如 m2.contains("zhaosi")
//  scala> m2("zhangsan")
//  res29: Int = 20
//
//  scala> m2("zhaosi")
//  java.util.NoSuchElementException: key not found: zhaosi
  // 还有一个办法就是使用get方法，可以避免报错，但是还是需要重新获取一次
//  scala> m2.get("zhaosi")
//  res36: Option[Int] = None
//
//  scala> m2.get("zhangsan")
//  res37: Option[Int] = Some(20)   // 注意此处使用了Option泛型
  // 最终的解决方案为getOrElse方法，如果取不到，返回默认值
//  scala> m2.getOrElse("zhaosi", 0)
//  res38: Int = 0

  // map的加减法
//  scala> m2 + ("zhaosi"->100)
//  res45: scala.collection.immutable.Map[String,Int] = Map(zhangsan -> 20, lisi -> 30, zhaosi -> 100)
//
//  scala> m2 - ("zhangsan")
//  res46: scala.collection.immutable.Map[String,Int] = Map(lisi -> 30)

  def main(args: Array[String]): Unit = {
    println("m1 print: " + m1)
    println("m2 print: " + m2)
  }
}
