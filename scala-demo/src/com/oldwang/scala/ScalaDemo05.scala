package com.oldwang.scala

/**
 * 条件表达式
 */
object ScalaDemo05 {
  def main(args: Array[String]): Unit = {
    val a = 1;
    val b = 5;
    if(a < b){
      println("a is less than b")
    }else{
      println("b is less than a")
    }

    val result:Boolean = {a > b}
    if(a > 2){
      println("a > 2")
    }else{
      println("a < 2")
    }

    val c = 5;
    if(a < c){
      println("a < c")
    }else{
      println("a > c")
    }
    if(b < c){
      println("b < c")
    }else{
      println("b > c")
    }

  }

}
