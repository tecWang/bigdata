package org.examples.scala.base

// 偏函数是函数的一种
// 因此，正常可以穿函数的地方，也都可以传偏函数

object FFunctionPartialFunction {
  // 偏函数的括号中两个参数，一个输入参数，一个输出参数
  def doubleValue: PartialFunction[Int, Int] = {
    case x:Int => x*2
    case _ => 0
  }

  def pickNumber: PartialFunction[Int, Int] = {
    case x:Int if (x % 2 == 0) => x * 2
    case x:Int if (x % 2 != 0) => x
    case _ => 0
  }

  // 偏函数用于类型过滤
  def filterType:PartialFunction[Any, Int] = {
    case x:Int => x*2
  }
  def filterTypeNormal(x:Any):Int = {
    x match {
      case x: Int => x*2
    }
  }

  def main(args: Array[String]): Unit = {

    println(doubleValue(10))        // 20

//    println(doubleValue(10.1))    // type mismatch

    println(pickNumber(10), pickNumber(21))     // (20,21)

    val arr = Array(1,2,3,4,5,"6",1.2)
    // 偏函数效率会更高
    // 模式匹配同时使用在普通函数、偏函数时，
    val res1 = arr.map({
      case x:Int => x*2
      case x:String => x
      case _ => 0   // 用于处理1.2的特殊值，否则执行会报错
    })
    res1.foreach(println)

    // 验证偏函数可以自动过滤数据的特性
    arr.collect(filterType).foreach(print)    // 246810, 不合规的数据会被剔除掉
//    arr.collect(item => filterTypeNormal(item))    // type mismatch

  }
}
