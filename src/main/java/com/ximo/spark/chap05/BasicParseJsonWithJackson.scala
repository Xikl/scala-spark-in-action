package com.ximo.spark.chap05

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark._

/**
  *
  *
  * @author 朱文赵
  * @date 2019/2/15 11:11 
  */
object BasicParseJsonWithJackson extends App {

  if (args.length < 3) {
    println("Usage: [sparkmaster] [inputfile] [outputfile]")
    System.exit(1)
  }

  val master = args(0)
  val inputFile = args(1)
  val outputFile = args(2)

  val sc = new SparkContext(master, BasicParseJsonWithJackson.getClass.getName,
    System.getenv("SPARK_HOME"))

  val inputRDD = sc.textFile(inputFile)

  // mapPartitions 详见第六章
  val result = inputRDD.mapPartitions(records => {
    // 创建json解析mapper
    // 使用with 关键字让Mapper 具有 scalaObjectMapper的特质
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    records.flatMap(record => {
      try {
        val person = mapper.readValue(record, classOf[Person])
        // 没有异常的时候 返回Some(person) Some 一定有值
        Some(person)
      } catch {
        // 遇到错误的时候返回空列表
        case _: Exception => None
      }
    })
  }, preservesPartitioning = true)
  result.filter(_.lovesPanders).mapPartitions(records => {
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    records.map(mapper.writeValueAsString(_))
  }).saveAsTextFile(outputFile)


}

case class Person(name: String, lovesPanders: Boolean)