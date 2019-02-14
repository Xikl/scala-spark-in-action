package com.ximo.spark.chap04

import org.apache.spark.{HashPartitioner, SparkContext}

/**
  *
  *
  * @author 朱文赵
  * @date 2019/2/14 15:14 
  */
object DataPartition {

  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "DataPartition", System.getenv("SPARK_HOME"))

    // 指定分区
    // 如果没有持久化 那么每次调用该rdd的时候都会重复的进行hash求值 将和没有partitionBy之前效果一样
    val userData = sc.sequenceFile[Int, String]("hdfs://...")
      .partitionBy(new HashPartitioner(100))
      .persist()

    def processNewLogs(logFileName: String): Unit ={
      val events = sc.sequenceFile[Int, String](logFileName)
      val joined = userData.join(events)
      // 假设我们过滤地址中存在用户名的
      // 然后计数
      val userAddessCount = joined.filter {
        case (userId, (username, address)) => address.contains(username)
      }.count()
      println(userAddessCount)
    }

  }



}
