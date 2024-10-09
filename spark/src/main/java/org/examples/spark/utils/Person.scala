package org.examples.spark.utils

// 抽象类
  //
abstract class Person(val age:Int, val name:String)
  extends Serializable {

  def sayHi() = {
    println(s"hi, i am ${this.name}, my age is ${this.age}")
  }

  // 定义一个方法，让后人去实现
  def jump()

}
