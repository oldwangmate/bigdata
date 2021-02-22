package com.oldwang.scala

/**
 * 表达式
 */
object ScalaDemo04 {
  def main(args: Array[String]): Unit = {
    val feetPerMile = 5280
    val yearPerMile = feetPerMile / 3.0
    val result = yearPerMile / 2000
    println(result)

    val result1 = {
      val feetPerMile = 5280
      val yearPerMile = feetPerMile / 3.0
      yearPerMile / 2000
    }
    println(result1)
  }
}
