package com.ximo.spark.chap05

/**
  *
  *
  * @author 朱文赵
  **/
object PatternMatch extends App {

  val currency: Money = RMB(12.3, "￥")

  currency match {
    case Dollar(value: Double) => println("Dollar:" + value)
    case RMB(value: Double, unit: String) => println(value + unit)
    case _ => println("else")
  }




}

/**
  * case class 不用new 直接用
  *
  */
abstract class Money

case class Dollar(value: Double) extends Money

case class RMB(value: Double, unit: String) extends Money
