package org.examples.scala.base

//  list在scala中不常用
import scala.collection.mutable.ListBuffer

object BDataStructList {

//  scala> val list = List[Int](1,2,3,4,5)
//  list: List[Int] = List(1, 2, 3, 4, 5)
//
//  scala> list(0)
//  res4: Int = 1

//  list拼接字符串，只能往前边拼接，后边会报错
  // list本身不可变，会生成新的list并返回
//  scala> 123 :: list
//  res0: List[Int] = List(123, 1, 2, 3, 4, 5)
//  scala> list :: 123
//  <console>:13: error: value :: is not a member of Int

//  list拼接list
//  scala> list :: list
//  res1: List[Any] = List(List(1, 2, 3, 4, 5), 1, 2, 3, 4, 5)

    val listbuffer = ListBuffer[Int](1, 2, 3, 4)

    val listBuffer2 = new ListBuffer[Int]()
// listbuffer是可以直接作用于buffer本身的
//  scala> listBuffer2+= 2
//  scala> listBuffer2+= 3
//  res8: listBuffer2.type = ListBuffer(2, 3)
//
//  scala> listBuffer2 ++= ListBuffer(11,22,33)
//  res9: listBuffer2.type = ListBuffer(2, 3, 11, 22, 33)

//  scala> listBuffer2.insert(3,44,55)
//
//  scala> listBuffer2
//  res11: scala.collection.mutable.ListBuffer[Int] = ListBuffer(2, 3, 11, 44, 55, 22, 33)

  // 转换
//  scala> val arr = Array(1,2,3)
//  arr: Array[Int] = Array(1, 2, 3)
//
//  scala> arr.toBuffer
//  res12: scala.collection.mutable.Buffer[Int] = ArrayBuffer(1, 2, 3)
//
//  scala> res12.toList
//  res13: List[Int] = List(1, 2, 3)

  // listbuffer可以转换为任何对象，但是没有任何对象可以转换为listbuffer
//  scala> listbuffer.toList
//  res14: List[Int] = List(1, 2, 3, 4)
//
//  scala> listbuffer.toArray
//  res15: Array[Int] = Array(1, 2, 3, 4)
//
//  scala> listbuffer.toBuffer
//  res16: scala.collection.mutable.Buffer[Int] = ArrayBuffer(1, 2, 3, 4)


  def main(args: Array[String]): Unit = {

  }
}
