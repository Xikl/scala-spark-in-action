package com.ximo.javaspark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * @author 朱文赵
 * @date 2019/1/17 16:25
 */
public class JavaSparkExample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("scala-spark-in-action");
        SparkContext sparkContext = new SparkContext(sparkConf);

    }



}
