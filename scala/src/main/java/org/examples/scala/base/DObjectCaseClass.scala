package org.examples.scala.base

class NormalCat(val name:String, val sex:String)

// 普通类与样例类的区别
// case关键字会自动生成伴生对象，且会自动帮实现apply toString等方法
  // 且不需要使用new关键字声明对象了
  // 对象的比较方式也会更改
case class CaseCat(val name:String, val sex:String)

// 此处可以用来定义一些常量类
// case object CaseCat {}

object DObjectCaseObject {
  def main(args: Array[String]): Unit = {
    val c0 = new NormalCat("handou", "M")
    val c1 = CaseCat("zhangxiaozhi", "FM")
    val c2 = CaseCat("zhangxiaozhi", "FM")
    val c3 = CaseCat("wangdalian", "FM")
    println(c0, c1, c2, c3)
    // 可以看出，使用case之后，同样属性的参数会生成同样的类
    // 并且样例类有toString方法，而非直接打印地址
//    (com.learn.base.NormalCat@42d8062c,CaseCat(zhangxiaozhi,FM),CaseCat(zhangxiaozhi,FM),CaseCat(wangdalian,FM))
  }
}
