package org.examples.scala.base

// 案例2
class Teacher (val name:String, val age:Int) {}

// class跟object互为伴生关系
// 每个类都可以有伴生对象，伴生类与伴生对象必须写在同一个文件中
// 类和伴生对象之间可以互相访问对方的私有方法和属性
// object用于其静态只初始化一次的特性，适合存储一些常量，避免空间占用等
object Teacher {
  // 只可以定义函数，不可以定义方法
    // Only classes can have declared but undefined members
  def speak = println("hello world !!!")
}


// 案例2
// private后边可以加不同的修饰符
//class School(private val salary:Int, val name:String) {
//class School(private[this] val salary:Int, val name:String) {
// 此处也可以写包名，但是不能写包的完整路径，也不能写包的子路径，对于本case，可以写com/learn/base
class School(private[scala] val salary:Int, val name:String) {
  // 构造函数前边也可以加 private 关键字
//class School private (private[learn] val salary:Int, val name:String) {
  // class前边也可以加 private 关键字
//private class School(private[learn] val salary:Int, val name:String) {
  private def sayHi = println("hi")
  def sayHiV2 = println("hi v2")
}
object School {
  val s = new School(10000, "tecwang")
  println("s.salary " + s.salary)
}

object DObjectAssociatedObject {
  def main(args: Array[String]): Unit = {

    // 案例1
    val t = new Teacher("tecwang", 28)
    Teacher.speak

    // 案例2
    val s = new School(20000, "tecwang_main")
    s.sayHiV2     // 只能访问V2，sayHi因为是私有函数，所以不能访问
//    s.salary    // Symbol salary is inaccessible from this place
    println("s.salary " + s.salary)
  }
}
