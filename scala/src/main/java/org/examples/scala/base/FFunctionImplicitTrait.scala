package org.examples.scala.base

/*
* comparable comparator
* java中可以使用comparable接口
* 可以直接new一个comparator比较器
*
* scala中，也有两类接口，ordered ordering
*
* */


// V1
// extends Ordered需要在Ordered后边指明类型
//class Actor (val name:String, val age:Int) extends Ordered[Actor] {
//  override def compare(that: Actor): Int = that.age - this.age
//}

// V2
//class Actor (val name:String, val age:Int)

// V3
//class Actor (val name:String, val age:Int)
//object MyActor {
//  // 隐式对象需要在另一个对象内部声明
//  implicit object ActorOrdering extends Ordering[Actor] {
//    override def compare(x: Actor, y: Actor): Int = x.age - y.age
//  }
//}

// V4
class ActorLearn(val name:String, val age:Int)
object MyActorLearn {
  // 依靠隐式转换实现
  // 位置需要放在最上边
  implicit def ActorOrderingv2(x:ActorLearn) = new Ordered[ActorLearn] {
    override def compare(that: ActorLearn): Int = that.age - x.age
  }
}


object FFunctionImplicitTrait {
  def main(args: Array[String]): Unit = {
    val a1 = new ActorLearn("liu DeHua", 35)
    val a2 = new ActorLearn("lady gaga", 40)
    val a3 = new ActorLearn("Zhu XiuLan", 33)

    val arr:Array[ActorLearn] = Array(a1, a2, a3)

    // V1
//    arr.sorted.foreach(t => println(t.name, t.age))

    // V2
    // 除了已有一个比较规则外，临时new一个比较规则也是可以的
//    arr.sorted(new Ordering[Actor] {
//      override def compare(x: Actor, y: Actor): Int = x.age - y.age
//    }).foreach(t => println(t.name, t.age))

    // V3
    // 通过隐式对象的方式也可以使sorted支持排序
      //   sorted的内部也是靠Ordering实现的
      //   def sorted[B >: A](implicit ord: Ordering[B]): Repr = {
//    import com.learn.base.MyActor.ActorOrdering
//    arr.sorted.foreach(t => println(t.name, t.age))
//    println(implicitly[MyActor.ActorOrdering.type ])    // com.learn.base.MyActor$ActorOrdering$@dfd3711

    // V4
//    arr.sorted.foreach(t => println(t.name, t.age))
  }
}

