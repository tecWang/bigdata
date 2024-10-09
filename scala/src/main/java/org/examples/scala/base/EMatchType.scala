package org.examples.scala.base

object EMatchType {
  def main(args: Array[String]): Unit = {

    // 需要注意的是，此处需要用Any来定义类型，否则下边match中会报错。
    val value:Any = 20

    if(value.isInstanceOf[Int]) println("Int")
    else println("Else")

    value match {
      case x:Int if(x > 18) => println(value, "Int ge 18")
      case x:Int => println(value, "Int le 18")
      case x:String => println(value, "String")
      case x:Bird => println(value, "Bird")
      case _ => println("Else")
    }

  }
}

class Bird


