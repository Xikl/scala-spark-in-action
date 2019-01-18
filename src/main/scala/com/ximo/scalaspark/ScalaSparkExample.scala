package com.ximo.scalaspark

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author 朱文赵
  */
class ScalaSparkExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("scala-spark-in-action")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("README.md")
    lines.flatMap(line => line.split(" ")).groupBy(identity).count()



  }

}
