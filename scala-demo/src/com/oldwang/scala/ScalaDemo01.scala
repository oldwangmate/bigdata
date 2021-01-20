package com.oldwang.scala

object ScalaDemo01 {

  def main(args: Array[String]): Unit = {
    //存储（并打印）值 17
    val number = 17
    println(number)
    //对于刚刚存储的值 17 尝试将其修改为20 会发生什么？ 编译报错 Reassignment to val
    //number = 20

    //存储（并打印）值 ABC1234
    val str = "ABC1234"
    println(str)

    //存储（并打印）值 15.4
    val whole = 15.4
    println(whole)
  }

}
