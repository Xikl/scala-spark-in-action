package com.ximo.spark.chap04

import java.net.URL

import org.apache.spark.Partitioner

/**
  *
  *
  * @author 朱文赵
  **/
object MyPartitioner {

  class DomainNamePartitioner(numParts: Int) extends Partitioner {
    override def numPartitions: Int = numParts

    override def getPartition(key: Any): Int = {
      // java url 类 获得域名
      val domain = new URL(key.toString).getHost
      // 字符串 计算hashCode
      val hashCode = domain.hashCode % numPartitions
      if (hashCode < 0) {
        hashCode + numPartitions
      } else {
        hashCode
      }
    }

    /**
      * 模式匹配
      *
      * @param other 另一个分区对象
      * @return
      */
    override def equals(other: Any): Boolean = other match {
      // 这个 instanceOf 具有一样的效果
      // other.isInstanceOf[DomainNamePartitioner] scala 下的
      case dnp: DomainNamePartitioner => dnp.numPartitions == numPartitions
      case _ => false
    }
  }


}
