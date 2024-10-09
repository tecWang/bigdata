package org.examples.scala.base

object CMethodReduce {
  def main(args: Array[String]): Unit = {
    val arr = List(1,2,3,4,5,6,7,8,9,10)

//    scala> val arr = List(1,2,3)
//    arr: List[Int] = List(1, 2, 3)
//
      // reduce的作用是输入两个元素，并做处理
      // map负责遍历，reduce负责聚合!
      // reduce要求输出，输出类型一致，reduceLeft不要求三个值完全一致
//    scala> arr.reduce
//    def reduce[A1 >: Int](op: (A1, A1) => A1): A1
//    scala> arr.reduceLeft
//    override def reduceLeft[B >: Int](op: (B, Int) => B): B

    println(arr.reduce((a, b) => a + b))    // 55，其实是实现了累加
    println(arr.reduce((a, b) => if (a > b)  a else b))    // 10

    val arr2 = List(("a", 10), ("b", 20), ("c", 30))
//    println(arr2.reduce((a, b) => a._2 + b._2))   // type mismatch; found   : Int; required: (String, Int)
    // reduce要求输出的参数必须与输入的泛型一致，即必须也是元组
    println(arr2.reduce((a, b) => ("", a._2 + b._2)))

//    ====================================================================
    // reduce left, reduce right
    // reduce 跟 reduceLeft的计算顺序默认是一样的，但是reduce可以并行执行，left right不可以
    // reduceLeft reduceRight的执行顺序不同
    println(arr.reduceLeft((a,b) => a+b))   // 55
    println(arr.reduceRight((a,b) => a+b))  // 55

    println(arr.reduceLeft((a,b) => {
      println(a, b, a-b)
      a-b
    }))   // -53
    //    (1,2,-1)
    //    (-1,3,-4)
    //    (-4,4,-8)
    //    (-8,5,-13)
    //    (-13,6,-19)
    //    (-19,7,-26)
    //    (-26,8,-34)
    //    (-34,9,-43)
    //    (-43,10,-53)
    println(arr.reduceRight((a,b) => {
      println(a, b, a-b)
      a-b
    }))  // -5
    //    (9,10,-1)
    //    (8,-1,9)
    //    (7,9,-2)
    //    (6,-2,8)
    //    (5,8,-3)
    //    (4,-3,7)
    //    (3,7,-4)
    //    (2,-4,6)
    //    (1,6,-5)
  }
}
