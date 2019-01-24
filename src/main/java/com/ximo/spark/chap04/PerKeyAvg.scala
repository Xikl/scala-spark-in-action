package com.ximo.spark.chap04

import org.apache.spark._
import org.apache.spark.SparkContext._

/**
  *
  *
  * @author 朱文赵
  * @date 2019/1/24 14:29
  */
object PerKeyAvg {


  def main(args: Array[String]): Unit = {

    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "PerKeyAvg", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(("coffee", 1), ("coffee", 2), ("panda", 4)))

    // 学习combineByKey
    // 先将value 变为一个 （value, 1）
    // 再累加 第一个累加 value 第二个累加次数
    // 再 合并
    val result = input.combineByKey(
      value => (value, 1),
      (acc: (Int, Int), value) => (acc._1 + value, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map { case (key, value) => (key, value._1 / value._2.toFloat) }
    result.collectAsMap().foreach(println(_))

  }



}
