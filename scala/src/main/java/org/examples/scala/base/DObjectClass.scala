package org.examples.scala.base

import scala.beans.BeanProperty

// 构造器，直接在class后边声明，不需要声明构造方法
// 构造器在new class的时候被调用，用于初始化对象
// 声明的时候如果是val，后续属性不能更改
// 构造器的变量如果不加 val/var，则该变量会被认为是一个参数，而非一个属性，只有类内部能用，外部 s.addr的方式无法访问
// 构造器中使用val，var的区别不大
class Student(
               val id:Int,
               var addr:String,
               val company:String = "cmbc"    // 构造器中的默认值参数
             ) {

  // scala没有get set方法，所有属性都是public的，直接s.age就可以实现get/set
  val nn:String = "zhangsan"    // 声明一个常量，后续不能被修改
  var sex:String = _    // 声明一个待赋值的属性
  @BeanProperty var age:Int = _

  // 辅助构造器
  def this(id:Int, addr:String, sex:String, company:String){
    this(id:Int,addr:String,company:String)
    this.sex = sex
  }

  def sayHello () = {
    println("hello " + nn)
  }

  //  override def toString: String = "name = " + nn + ",age = " + age + ",sex = " + sex
  override def toString: String = s"name = ${nn}; sex = ${sex}; addr = ${addr}; company = ${company}"
}

object DClassNew {
  def main(args: Array[String]): Unit = {

    val student = new Student(id = 1, addr = "beijing", company = "hainiu")

    println(student, student.nn)
//    (name = zhangsan; sex = null; addr = beijing; company = hainiu,zhangsan)

    // 调用方法
    student.sex = "M"   // set属性
    student.setAge(20)  // 调用set方法
    student.sayHello()
    println(student.toString)
//    name = zhangsan; sex = M; addr = beijing; company = hainiu

  }
}
