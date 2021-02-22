package com.oldwang.scala

/**
 * 计算顺序
 */
object ScalaDemo06 {
  def main(args: Array[String]): Unit = {
    val kg:Double = 196/2;
    val height = 1.81
    val bmi = kg /(height * height)
    if(bmi < 18.5){
      println("轻了")
    }else if(bmi < 25){
      println("合适")
    }else{
      println("重了")
    }

    val sunny = true
    val partly_cloudy = true;
    val temp = 80;
    val temp1 = 21
    if(sunny && temp >= 80){
      println(true)
    }

    if(sunny || partly_cloudy && temp > 80){
      println(true)
    }

    if(sunny || partly_cloudy && temp > 80 || temp1 > 21 ){
      println(true)
    }

    val result:Double = (temp - 32) * 5.0 / 9.0
    println(result)

    val result1:Double = ((result * 9.0)  /  5.0 )+ 32
    println(result1)
  }
}
