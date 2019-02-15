package com.ximo.spark.chap05

import org.apache.spark.SparkContext

/**
  *
  *
  * @author 朱文赵
  * @date 2019/2/15 9:52 
  */
object ReadFile {

  def wholeTextFile(sc: SparkContext): Unit ={
    val fileRDD = sc.wholeTextFiles("your file path")
    // wholeTextFiles 返回一个pair rdd key为输入文件的文件名
    val result = fileRDD.mapValues(line => {
      val nums = line.split(" ").map(_.toDouble)
      nums.sum / nums.length.toDouble
    })



  }




}
