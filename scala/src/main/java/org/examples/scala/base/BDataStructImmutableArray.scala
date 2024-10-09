package org.examples.scala.base

object BDataStructImmutableArray {

  val arr:Array[Int] = Array[Int](1,2,3,4,5,6)
  val arr2 = Array(1,2,3,4,5,6)

  val arr3= new Array(5)
//  scala> val arr = new Array(5)
//  Nothing无法被选中，无法被赋值，建议使用arr4得赋值方式
//  arr: Array[Nothing] = Array(null, null, null, null, null)

  val arr4 = new Array[Int](5)
//  指定类型后，会按照该类型得默认值赋值
//  scala> val arr4 = new Array[Int](5)
//  arr4: Array[Int] = Array(0, 0, 0, 0, 0)

  val arr5 = Array(1, 2, 3, true)
//  scala> val arr5 = Array(1, 2, 3, true)
//  1, true都存在得时候，数组的类型会变成anyval
//  arr5: Array[AnyVal] = Array(1, 2, 3, true)

  val arr6 = Array(1, 2, 3, true, "123")
//  scala> val arr6 = Array(1, 2, 3, true, "123")
//  泛型中基础数据类型，引用数据类型都存在，数组类型变成any
//  arr6: Array[Any] = Array(1, 2, 3, true, 123)

  // 二维数组
  val arr7 = Array(Array(1,2,3), Array(1, true, "b", "c"))
//  scala> val arr7 = Array(Array(1,2,3), Array(1, true, "b", "c"))
//  arr7: Array[Array[_ >: Int]] = Array(Array(1, 2, 3), Array(1, true, b, c))

  val arr8 = Array(1, 2, 3, Array(1, 2, 3))   // 此处其实声明了一个一维数组，跟arr7不一样A
//  scala> arr8(3)
//  res4: Any = Array(1, 2, 3)
//  scala> arr8(3)(2)
//  <console>:13: error: Any does not take parameters
//  需要将该元素强转成Array[Int]才能读取
//  scala> arr8(3).asInstanceOf[Array[Int]](2)
//  res6: Int = 3

//  Array下边是有很多方法的，命令行输入"Array."后按TAB就可以显示
//  scala> Array.
//    apply          concat   empty               emptyByteArray   emptyDoubleArray   emptyIntArray    emptyObjectArray   fallbackCanBuildFrom   iterate      ofDim   tabulate
//  canBuildFrom   copy     emptyBooleanArray   emptyCharArray   emptyFloatArray    emptyLongArray   emptyShortArray    fill                   newBuilder   range   unapplySeq

//  Array.ofDim[Int](2,3)
//  scala> Array.ofDim[Int](2,3)
//  res12: Array[Array[Int]] = Array(Array(0, 0, 0), Array(0, 0, 0))


//  数组的方法有很多
//  scala> arr.
//  ++            array           copyToBuffer   endsWith    foreach           init                 lastOption      orElse             reduceOption        scanRight       startsWith     toIndexedSeq    transpose
//  ++:           canEqual        corresponds    exists      genericBuilder    inits                length          padTo              reduceRight         segmentLength   stringPrefix   toIterable      union
//  +:            clone           count          filter      groupBy           intersect            lengthCompare   par                reduceRightOption   seq             sum            toIterator      unzip
//  /:            collect         deep           filterNot   grouped           isDefinedAt          lift            partition          repr                size            tail           toList          unzip3
//  :+            collectFirst    diff           find        hasDefiniteSize   isEmpty              map             patch              reverse             slice           tails          toMap           update
//  :\            combinations    distinct       flatMap     head              isTraversableAgain   max             permutations       reverseIterator     sliding         take           toSeq           updated
//  addString     companion       drop           flatten     headOption        iterator             maxBy           prefixLength       reverseMap          sortBy          takeRight      toSet           view
//  aggregate     compose         dropRight      fold        indexOf           last                 min             product            runWith             sortWith        takeWhile      toStream        withFilter
//  andThen       contains        dropWhile      foldLeft    indexOfSlice      lastIndexOf          minBy           reduce             sameElements        sorted          to             toTraversable   zip
//  apply         containsSlice   elemManifest   foldRight   indexWhere        lastIndexOfSlice     mkString        reduceLeft         scan                span            toArray        toVector        zipAll
//  applyOrElse   copyToArray     elemTag        forall      indices           lastIndexWhere       nonEmpty        reduceLeftOption   scanLeft            splitAt         toBuffer       transform       zipWithIndex

//  数组比较常见的方法有：
//  val arr:Array[Int] = Array[Int](1,2,3,4,5,6)
//  contains
  //  scala> arr.contains(1)
  //  res15: Boolean = true
//  reverse
//  distinct  去重
//  length min max sum
  //  scala> arr.length
  //  res14: Int = 6
//  size
//  slice take
  //  scala> arr.slice(3,5)
  //  res20: Array[Int] = Array(4, 5)
  //  scala> arr.take(4)
  //  res18: Array[Int] = Array(1, 2, 3, 4)
// mkString
  //  scala> arr.mkString("+")
  //  res17: String = 1+2+3+4+5+6
// +
  // scala> 111+:arr
  // res22: Array[Int] = Array(111, 1, 2, 3, 4, 5, 6)
  // scala> arr:+999
  // res23: Array[Int] = Array(1, 2, 3, 4, 5, 6, 999)

// for循环
// for(i <- arr) println(i)

  def main(args: Array[String]):Unit = {
    println("arr: " + arr)
    println("arr2: " + arr2)
    println("arr3: " + arr3)

    println("arr4: " + arr4)
    println("arr4 select:" + arr4(4))   // 选取元素要用小括号，而不是中括号
    arr4(4) = 9999
    println("arr4 set:" + arr4(4))

    println("arr7 select: " + arr7(1)(2))   // 二维数组筛选数据
  }
}
