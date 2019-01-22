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
    val rdd = sc.parallelize(List(1, 2, 3))
    val ints = rdd.map(s => s * s).collect()

    rdd.aggregate(0, 0)(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    rdd.reduce((x , y) => x + y)
    // scala 中的fold操作 反人类？？、
    rdd.fold(0) (_+_)

    val lines = sc.parallelize(List("hello world", "word", "python"))
    lines.flatMap(line => line.split(" "))
      .first()
  }

  def testCount(sc: SparkContext): Unit= {
    val pythonRdd = sc.textFile("").filter(line => line.contains("python"))
    println("python lines : ", pythonRdd.count())
    println("前十个", pythonRdd.take(10))

  }


}
