package com.oldwang.scala

/**
 * scala 变量
 */
object ScalaDemo03 {

  def main(args: Array[String]): Unit = {

    // 创建一个Int值（val）并将其值设置为5 尝试将其修改为10 会发生什么 Reassignment to val  怎么解决 将其修改为var
    var number : Int = 5
     number = 10;

    // 创建一个名字 v1的Int变量（var） 并将其设置为5 再将其更新为10 并存储到名字constantv1的val中可行吗？可行  有什么用途？ 可以通过计算得出常量
    var v1 : Int = 5
    v1 = 10;
    val constatv1 = v1;

    //将上面v1的值更新为15 constatv1的是否会发生变化？ 不会 因为是常量
    v1 = 15;
    println(constatv1)

    //创建一个名为v2的Int变量 初始值为v1 将v1的更新为20 v2是否发生了变化  没有发生变化 因为是先赋值
    var v2:Int = v1
    v1 = 20
    println(v2)
  }

}
