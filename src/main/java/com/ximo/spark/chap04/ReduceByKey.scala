package com.ximo.spark.chap04

import org.apache.spark.SparkContext

/**
  *
  *
  * @author 朱文赵
  * @date 2019/1/24 16:23 
  */
object ReduceByKey {


  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "PerKeyAvg", System.getenv("SPARK_HOME"))

    val data = Seq(("a", 3), ("b", 4), ("a", 1))

    // 指定分区 reduce的第二个参数
    val rdd = sc.parallelize(data)
    rdd.reduceByKey((value1, value2) => value1 + value2, 10)
    //  查看 RDD 的分区数
    rdd.partitions.length






  }

  /**
    * scala 隐式转化
    * @param sc SparkContext
    */
  def sortByKey(sc: SparkContext) : Unit={
    // 排序
    val sortData = List((111, "python"), (22222, "scala"))
    val sortDataRDD = sc.parallelize(sortData)
    implicit val sortIntByString = new Ordering[Int]{
      override def compare(x: Int, y: Int): Int = x.toString.compareTo(y.toString)
    }

    sortDataRDD.sortByKey()
  }



}
