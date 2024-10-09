package org.examples.scala.base

import scala.collection.mutable

object BDataStructSet {

//  scala> val set = Set(1,2,2,3,4,5)
  // set自动会去重
  // set中的元素是无序的
//  set: scala.collection.immutable.Set[Int] = Set(5, 1, 2, 3, 4)

  // set可以查看是否包含该元素, set(4)的含义不是取第五个元素的下表
//  scala> set(4)
//  res19: Boolean = true
//
//  scala> set(94)
//  res20: Boolean = false

  // set.max
  // set.min

  // set可以添加/删除元素
//  scala> set + 99
//  res23: scala.collection.immutable.Set[Int] = Set(5, 1, 2, 3, 99, 4)

  // ---------------------------------------
  // 可变set

//  scala>   import scala.collection.mutable.Set
//  import scala.collection.mutable.Set
//
//  scala>   val set = Set[Int]()
//  set: scala.collection.mutable.Set[Int] = Set()
//
//  scala> set += 1
//  res24: set.type = Set(1)
//
//  scala> set ++= Set(1, 22, 22, 3, 44, 44)
//  res25: set.type = Set(1, 3, 22, 44)
//
//  scala> set.remove(22)
//  res26: Boolean = true

  // ---------------------------------------
//  import scala.collection.immutable.HashSet
  val set = new mutable.HashSet[Int]()
//  scala> set += 1
//  res27: set.type = Set(1)
//
//  scala> set.add(2)
//  res28: Boolean = true

  // 如果可变 + 不可变的hashset都引入了，自动会使用可用hashset
  // 如果两个都想用
//  val set1 = new MHashSet[Int]()
//  val set2 = new IMHashSet[Int]()

  def main(args: Array[String]): Unit = {

  }
}
