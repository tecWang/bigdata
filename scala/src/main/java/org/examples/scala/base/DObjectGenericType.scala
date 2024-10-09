package org.examples.scala.base

/*
*  + 表示协变 返回值是协变. 在协变下，A是父类，B是子类，是正向的
*    class B
*  - 表示逆变 参数是逆变. 在逆变下，B是父类，A是子类
* */

class A

class B extends A

class C extends B

// 泛型上下限，冒号放右边
class Computer[T<:B] (val device:T)
//class Computer[T>:B] (val device:T)

object DObjectGenericType {

  def getFunc(f:(B) => B) = f

  def main(args: Array[String]): Unit = {

    val c = new Computer[B](new B)
//    val c2 = new Computer[A](new A)   // Type A does not conform to upper bound B of type parameter T
    val c3 = new Computer[C](new C)

    // OK
    // 返回值可以放B的子类，此时可以传B, C
    // 参数永远是大于等于返回值的
    getFunc((x:B) => new B)
    getFunc((x:B) => new C)
    // NOT OK
//    getFunc((x: B) => new A)    // type mismatch

    // OK
    // 参数值可以放B的子类，此时可以传A, B
    getFunc((x:A) => new B)
    getFunc((x:B) => new B)
    // NOT OK
//    getFunc((x:C) => new B)     // type mismatch

  }
}
