package com.ximo.spark.chap03

import org.apache.spark.storage.StorageLevel
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

    val lines = sc.parallelize(List("hello world", "word", "python line hello"))
    val words = lines.flatMap(line => line.split(" "))
    // 持久化
    words.persist(StorageLevel.MEMORY_ONLY_2)

    // 默认为persist(StorageLevel.MEMORY_ONLY)
//    words.cache()
    println(words.count())
    println(words.first())
    // 手动删除缓存
    words.unpersist(true)

    // 最后会变成 <hello hello world>, <word word>, <python python line hello>
    val keyValueRDD = lines.map(words => (words.split(" ")(0), words))
    keyValueRDD.filter({case (key, value) => value.length > 10} )

    // 详见 pdf的部分 page 68
    keyValueRDD.mapValues(value => (value, 1))
      .reduceByKey((value1, value2) => (value1._1.concat(value2._1), value1._2 + value2._2))

    // word count 繁琐版本
    val input = sc.textFile(" ")
//    val words = input.flatMap(line => line.split(" "))
    val words2 = input.flatMap(_.split(" "))
    words2.map(word => (word, 1)).reduceByKey((value1, value2) => value1 + value2)

    // 简约版本
    sc.textFile(" ").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)




  }

  def testCount(sc: SparkContext): Unit= {
    val pythonRdd = sc.textFile("").filter(line => line.contains("python"))
    println("python lines : ", pythonRdd.count())
    println("前十个", pythonRdd.take(10))

  }


}
