package org.examples.scala.base

class Cat(val name:String, val sex:String)

object Cat {
  def apply(name:String, sex:String):Cat = new Cat(name, sex)
}

// 定义一个Dog类，并且将构造方法设定为private，只能通过apply构造新对象
class Dog private (val name:String, val sex:String) {
  // 伴生类中也有apply方法
  def apply(i:Int) = {
    if (i == 0) this.name
    else if (i == 1) this.sex
    else "attr not exists !!!"
  }
}

object Dog {
  var dog:Dog = _
  def apply(name:String, sex:String) = {
    if (dog == null)
      dog = new Dog(name, sex)
    dog
  }
  // 将对象拆分出对应的属性
  def unapply(dog:Dog) = Some(dog.name, dog.sex)
}

object DObjectApply {
  def main(args: Array[String]): Unit = {
    val cat = new Cat("zhangxiaozhi", "FM")
    // 以下两种方式都可以调用apply
    val cat2 = Cat.apply("wangdalian", "FM")
    val cat3 = Cat("tangyuan", "M")
    println(cat, cat2, cat3)
//    (com.learn.base.Cat@42d8062c,com.learn.base.Cat@6043cd28,com.learn.base.Cat@cb51256)

    //
    val dog = Dog.apply("handou", "M")
    val dog2 = Dog("kele", "M")
    println(dog, dog2, dog2(0), dog2(9))
//    (com.learn.base.Dog@4923ab24,com.learn.base.Dog@4923ab24,handou,attr not exists !!!)
    println("dog2.name: " + dog2.name)
    // 两个对象的值一样
    // 因为第二次调用apply的时候，dog对象已经存在了，因此直接返回了已有的对象，对应的name = handou

    //
    val Dog(name, sex) = Dog("pangpan", "FM")
    println(Dog)
//    com.learn.base.Dog$@43738a82

  }
}
