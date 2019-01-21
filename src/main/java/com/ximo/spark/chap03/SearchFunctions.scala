package com.ximo.spark.chap03

import org.apache.spark.rdd.RDD

/**
  *
  *
  * @author 朱文赵
  * @date 2019/1/21 16:10 
  */
class SearchFunctions {

  private val query: String = "python"

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchesFunctionReference(rdd: RDD[String]) : RDD[String] = {
    rdd.filter(line => isMatch(line))
  }



}
