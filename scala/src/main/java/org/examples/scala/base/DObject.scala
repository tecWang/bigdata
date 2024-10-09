package org.examples.scala.base

// 静态类/常量类，更适合存放工具，常量等内容
// 类加载的时候优先加载静态内容，且只加载一次
// object中的属性，方法都可以直接调用，不需要通过new来加载
// object声明的全部都是静态类
// object没有构造器，不需要实例化
//
object Test {
  println("init")
}

object DObject {
  def main(args: Array[String]): Unit = {
    // 三个对象都是同一个实例，说明只初始化了一次
    // init也只打印了一次
    println(Test)   // com.learn.base.Test$@7ce6a65d
    println(Test)   // com.learn.base.Test$@7ce6a65d
    println(Test)   // com.learn.base.Test$@7ce6a65d
  }
}
