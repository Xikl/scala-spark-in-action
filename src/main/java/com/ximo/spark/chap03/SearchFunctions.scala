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

  /**
    * isMatch 为this/
    *
    * @param rdd
    * @return
    */
  def getMatchesFunctionReference(rdd: RDD[String]): RDD[String] = {
    rdd.filter(line => isMatch(line))
  }

  /**
    * 拆分为数组
    * query 为 this.query 表示 所以会将this的应用传入多个节点中
    * 这个需要特殊考虑
    *
    * @param rdd 数据集
    * @return
    */
  def getMatchesFieldReference(rdd: RDD[String]): RDD[Array[String]] = {
    rdd.map(str => str.split(query))
  }

  /**
    * 这样方法中 就不会将 this的引用传递到各个节点中
    *
    * @param rdd
    * @return
    */
  def getMatchesNoReference(rdd: RDD[String]) : RDD[Array[String]] = {
    val query_ = query
    rdd.map(str => str.split(query_))
  }


}
