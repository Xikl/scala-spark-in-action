package com.ximo.spark.chap05

/**
  *
  *
  * @author 朱文赵
  * @date 2019/2/15 10:41 
  */
object CaseClassTest extends App {

  val bundle: Item = Bundle("Father's day special", 20.0,
    Article("Scala in action", 20.00),
    Bundle("other language", 10.0, Article("python", 20.87), Article("java", 12.0))
  )

  def price(item: Item): Double = item match {
    // 模式匹配
    case Article(_, price) => price
    // 递归调用 map(price)
    case Bundle(_, discount, items@_*) => items.map(price).sum - discount
  }


}

abstract class Item

case class Article(desc: String, price: Double) extends Item

case class Bundle(desc: String, discount: Double, items: Item*) extends Item
