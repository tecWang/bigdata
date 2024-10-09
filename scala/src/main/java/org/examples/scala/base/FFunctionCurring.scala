package org.examples.scala.base

object FFunctionCurring {

  def add(x:Int, y:Int) = x + y

  // 下边所有的操作都是基于add1函数进行的，需要先把函数拆分开
  def add1(x:Int)(y:Int) = x + y

  // 定义新的函数
  val add2 = (a:Int) => add1(a)_

  val add3 = (a:Int) => add1(a)(50)

  // implicit参数
  // 有且只能用一次
  //
  def circle(r:Int)(implicit pi:Double=3.14) = {
    r*r*pi
  }
  // 可以利用柯里化实现不同版本参数管理
  val circle2 = (r:Int) => circle(r)(pi=3.1415)
  val circle3 = (r:Int) => circle(r)(pi=3.141592653)

  def main(args: Array[String]): Unit = {

    println("add1(10)(20) " + add1(10)(20))   // 30

    // 此处返回了一个函数
    println("add2(10) " + add2(10))   // com.learn.base.FFunctionCurring$$$Lambda$6/0x00000007c0114428@6121c9d6

    println("add2(10)(20) " + add2(10)(20))   // 30

    println("add3(10) " + add3(10))   // 60

    // 计算圆周率
    println(circle(10)())   // 不传值，默认用3.14
    println(circle(10))     // 参数用implicit修饰之后，就可以不加括号也正常调用
    println(circle2(10))
    println(circle3(10))
//    314.0
//    314.15000000000003
//    314.1592653

    // 返回当前环境中double类型的默认值
    // 这些参数可以放到一个专门的util类里边，供使用，使用时import Util.pi 即可
    // 隐式属性可以充当隐式变量的值
    implicit val salary = 1000.1
    val c = implicitly[Double]
    println(c)    // 1000.1
  }
}
