package com.ximo.spark.chap02

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author 朱文赵
  */
object ScalaSparkExample {

  def main(args: Array[String]): Unit = {
    wordCount()
  }

  /**
    * 统计计数
    *
    */
  def wordCount(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("scala-spark-in-action")
    val sc = new SparkContext(conf)
    val input = sc.textFile("D:\\jupyter-notebook\\README.md")

    // filter 转化操作 transform
    val words = input
        .filter(line => line.contains("python"))
        .flatMap(line => line.split(" "))

    val counts = words.map(word => (word, 1)).reduceByKey((x, y) => x + y)
    println(counts.collectAsMap())
    sc.stop
  }


}
