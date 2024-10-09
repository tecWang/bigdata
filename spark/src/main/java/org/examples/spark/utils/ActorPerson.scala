package org.examples.spark.utils

// 同一个 package 下边的类不需要 import

// 样例类
class ActorPerson(override val age: Int, override val name: String)
  extends Person(age, name)
  with Serializable {

  // Class 'UtilActor' must either be declared abstract or implement abstract member 'jump(): Unit' in 'base. utils. UtilPerson'
  override def jump(): Unit = {
    println("i can jump 2 meters away.")
  }

  override def sayHi(): Unit = {
    println(s"override by UtilAction. i am ${this.name}, my age is ${this.age}")
  }
}
