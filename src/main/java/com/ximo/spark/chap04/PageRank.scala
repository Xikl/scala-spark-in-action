package com.ximo.spark.chap04

import org.apache.spark.{HashPartitioner, SparkContext}

/**
  *
  *
  * @author 朱文赵
  * @date 2019/2/14 16:25 
  */
object PageRank {

  def pageRank(sc: SparkContext, linkUrl: String): Unit = {
    val linksRDD = sc.objectFile[(String, Seq[String])](linkUrl)
      .partitionBy(new HashPartitioner(100))
      .persist()

    // 为每个链接初始化为1.0
    var ranks = linksRDD.mapValues(_ => 1.0)

    // 循环十次
    for (i <- 0 until 10) {
      // 这里采用flatMap 将Seq平摊开
      val contributions = linksRDD.join(ranks).flatMap{
        case (pageId, (links, rank)) =>
          links.map(link => (link, rank / links.size))
      }

      // 保存每次循环的值 将相同key的值进行相加 然后将value的值 进行一系列的操作
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(0.15 + 0.85 * _)
    }

    // 存储ranks
    ranks.saveAsTextFile("ranks")
  }




}
