package com.oldwang.scala

object ScalaDemo02 {

  def main(args: Array[String]): Unit = {

    //显示指定类型
    val n = 1
    val p = 1.2
    //可以声明：
    val nn:Int = 1
    val pp:Double = 1.2

    //scala 基本类型
    val whole:Int = 11 //表示integer
    val fractional:Double = 1.4 //表示double
    //true or false
    val trueOrFalse:Boolean = true //表示boolean
    val words:String = "abc" //表示String
    val lines:String =
      """a
        |b
        |c
        |""".stripMargin //出现多行文本的时候 使用三个双引号将他们括起来
    println(whole,fractional,words)
    println(lines)

    //1. 将5存储为Int并打印
    val number:Int = 5
    println(number)

    //2. 将ABCD123存储为String并打印
    val string:String = "ABCD1234"
    println(string)

    //3.将5.4存为Double并打印
    val double:Double = 5.4;
    println(double)

    //4. 存储true
    val boolean:Boolean = true
    println(boolean)

    //5.存储多行String 打印时会出现多行么?  会
    val line:String =
      """
        |aaa
        |bb
        |cc
        |""".stripMargin
    println(lines)

    //6.试图将 String "my" 存储到Boolean中会发生什么? 编译错误 类型匹配
    // val my:Boolean = "my"
    // println(my)
  }


}
