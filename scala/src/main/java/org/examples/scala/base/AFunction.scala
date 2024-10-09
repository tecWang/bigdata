package org.examples.scala.base

import java.io.File

object AFunction {
  //  自定义方法
  private def add(a:Int, b:Int):Int = {
    a + b
  }
  private def sayHello(): Unit = println("Hello")

  private def sayHelloV2(x:String): Unit = {
    if (x != null) {
      println(x)
    } else {
      println("hello")
    }
  }

  def listFiles (path:String): Unit = {
    val file = new File(path)
    if (file.isFile) {
      println(file.getPath)
    } else {
      println(file.getPath)
      val fs = file.list()
      for (f <- fs) {
        listFiles(path + "/" + f)
      }
    }
  }

  val add2 = (x:Int, y:Int) => x + y

  // 函数作为参数传入方法中
  def calculate (x:Int, y:Int, f: (Int, Int) => Int) = f(x, y)

  // 函数作为返回值
  def getFunc() = {
    val func = (x:Int, y:Int) => x + y
    func
  }

  // 函数作为返回值
  def getFuncV2(a:Int) = {
    val func = (x:Int, y:Int) => x * a + y
    func
  }

  // 函数方法转换，方法可以转换成函数
  // add _
  // calculate(70, 20, add)     // 方法作为参数传入时，自动会转化为函数
  // calculate(70, 20, add _)   // 或者手工转化为函数

  def main(args: Array[String]):Unit = {
    println(add(1,2))
    println("function add2 return: " + add(10,20))
    sayHello()
    sayHelloV2("base/test")
    //    listFiles("C:\\Users\\tecwang\\IdeaProjects\\scala\\src")

    // 函数作为参数
    println("calculate return:" + calculate(70, 20, add2))
    // 匿名函数
    println("calculate anonymous return:" + calculate(7, 22, (x:Int, y:Int) => x+y))

    println("getFunc result:" + getFunc()(1,2))
    println("getFunc result:" + getFuncV2(100)(1,2))
  }
}
