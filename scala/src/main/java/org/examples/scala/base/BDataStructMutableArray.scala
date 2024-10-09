package org.examples.scala.base

import scala.collection.mutable.ArrayBuffer

object BDataStructMutableArray {

  val buffer:ArrayBuffer[Int] = ArrayBuffer[Int] (1,2,3,4,5)
//  scala> val buffer:ArrayBuffer[Int] = ArrayBuffer[Int] (1,2,3,4,5)
//  buffer: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4, 5)

  val buffer2 = ArrayBuffer[Int](1,2,3,4,5)
//  scala>   val buffer2 = ArrayBuffer[Int](1,2,3,4,5)
//  buffer2: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4, 5)

  val buffer3 = new ArrayBuffer[Int](5)   // 此处限制5没有意义，数组本身长度就是不固定的
//  scala>   val buffer3 = new ArrayBuffer[Int](5)
//  buffer3: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer()

//  scala> buffer.
//    ++     :+            clear           count       flatMap           head           isEmpty              mapResult   permutations        remove            seq               stringPrefix   toIterator      trimStart
//    ++:    :\            clone           diff        flatten           headOption     isTraversableAgain   max         prefixLength        repr              size              sum            toList          union
//    ++=    <<            collect         distinct    fold              indexOf        iterator             maxBy       prepend             result            sizeHint          tail           toMap           unzip
//    ++=:   WithFilter    collectFirst    drop        foldLeft          indexOfSlice   last                 min         prependAll          reverse           sizeHintBounded   tails          toSeq           unzip3
//    +:     addString     combinations    dropRight   foldRight         indexWhere     lastIndexOf          minBy       product             reverseIterator   slice             take           toSet           update
//    +=     aggregate     companion       dropWhile   forall            indices        lastIndexOfSlice     mkString    reduce              reverseMap        sliding           takeRight      toStream        updated
//    +=:    andThen       compose         endsWith    foreach           init           lastIndexWhere       nonEmpty    reduceLeft          runWith           sortBy            takeWhile      toString        view
//    -      append        contains        equals      genericBuilder    inits          lastOption           orElse      reduceLeftOption    sameElements      sortWith          to             toTraversable   withFilter
//    --     appendAll     containsSlice   exists      groupBy           insert         length               padTo       reduceOption        scan              sorted            toArray        toVector        zip
//    --=    apply         copyToArray     filter      grouped           insertAll      lengthCompare        par         reduceRight         scanLeft          span              toBuffer       transform       zipAll
//    -=     applyOrElse   copyToBuffer    filterNot   hasDefiniteSize   intersect      lift                 partition   reduceRightOption   scanRight         splitAt           toIndexedSeq   transpose       zipWithIndex
//    /:     canEqual      corresponds     find        hashCode          isDefinedAt    map                  patch       reduceToSize        segmentLength     startsWith        toIterable     trimEnd

// +
  //  scala> buffer:+1
  //  res28: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4, 5, 1)
  //
  //  scala> 1+:buffer
  //  res29: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 1, 2, 3, 4, 5)
  //  不改变原有buffer，会生成新的元素
  //  scala> buffer
  //  res30: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4, 5)
// +=
  // += 会改变原有的buffer
  //  scala> buffer += 1
  //  res32: buffer.type = ArrayBuffer(1, 2, 3, 4, 5, 1)
  //
  //  scala> buffer
  //  res33: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4, 5, 1)
// ++=
  //  ++=可以添加多个元素
  //  scala> buffer ++= Array(111,222,333)
  //  res35: buffer.type = ArrayBuffer(1, 2, 3, 4, 5, 1, 111, 222, 333)
// --=
// insert
  //  scala> buffer
  //  在指定位置插入元素
  //  res38: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 123, 321, 3, 4, 5, 1, 111, 222, 333)
// remove
  //  scala> buffer.remove
  //  scala> buffer
  //  res40: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 123, 321, 3, 4, 5, 1, 111, 222, 333)
  //    def remove(n: Int): Int
      //  scala> buffer.remove(0)
      //  res41: Int = 1
      //  scala> buffer
      //  res42: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(2, 123, 321, 3, 4, 5, 1, 111, 222, 333)
  //    override def remove(n: Int,count: Int): Unit
      //  scala> buffer.remove(3,2)
      //  scala> buffer
      //  res44: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(2, 123, 321, 5, 1, 111, 222, 333)
// toArray toBuffer
  // 两类元素可以互相转换
  //  scala> buffer.toArray
  //  res45: Array[Int] = Array(2, 123, 321, 5, 1, 111, 222, 333)
  //
  //  scala> res45.toBuffer
  //  res46: scala.collection.mutable.Buffer[Int] = ArrayBuffer(2, 123, 321, 5, 1, 111, 222, 333)

  def main(args: Array[String]): Unit = {

  }
}
