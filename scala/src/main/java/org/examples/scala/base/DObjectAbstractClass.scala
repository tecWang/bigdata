package org.examples.scala.base

// 抽象类：
  // 与java类型，只是java没有属性的抽象，scala可以又属性的抽象
// 特质：
  // 可以理解为java中升级版的接口
  // java中接口不能声明没有值的属性和有实现的方法，scala可以声明没有值的属性和有实现的方法
// 重写
// 特质与抽象类的区别：
  //

// 抽象类
// 抽象类没有构造器，且不能直接new抽象类，应该new实现类，必须实现完才能new
abstract class Animal(val kind:String) {
  val age:Int
  val region = "China"
  def breath:String
  def move(direction:String):String
  def speak = println("speak")
}

// trait没有构造器
trait Fly {
  val height:String
  val speed:String = "100km/h"
  def fly = println("i can fly")
  def fly_way   // 抽象方法
}

// 继承父类
// scala只有extends
  // name是新的属性，所以要声明
  // kind是父类的属性，传入即可
class Duck(val name:String, kind:String) extends Animal(kind) with Fly{

//  override val height: String = ???
  val height:String = "123"     //

  override def fly_way: Unit = println("Duck flied into sky !!!")

  override def fly: Unit = println("Duck can fly too !!!")

  override val age: Int = 12

  override def breath: String = "Duck can fly"

  override def move(direction: String): String = "Duck moved"
}

object DObjectAbstractClass {
  def main(args: Array[String]): Unit = {
    val d = new Duck(name = "beijing duck", kind = "white duck")
    println(d, d.name)
//    (com.learn.base.Duck@7f690630,beijing duck)
    d.speak
//    speak
  }
}
