package com.ximo.spark.chap03

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @author 朱文赵
  * @date 2019/1/21 15:40 
  */
object ScalaParallelize {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("scala-spark-in-action")
    val sc = new SparkContext(conf)
    val parallelize = sc.parallelize(List(1, 2, 3))

  }

  def testCount(sc: SparkContext): Unit= {
    val pythonRdd = sc.textFile("").filter(line => line.contains("python"))
    println("python lines : ", pythonRdd.count())
    println("前十个", pythonRdd.take(10))

  }


}
