package com.ximo.spark.chap03;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.util.Arrays;

/**
 * @author 朱文赵
 * @date 2019/1/21 15:36
 */
public class ParallelizeList {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("scala-spark-in-action");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> parallelize = sparkContext.parallelize(Arrays.asList(1, 2, 3));

    }

    private static void testActionCountAndTake(JavaSparkContext sparkContext) {
        JavaRDD<String> textFile = sparkContext.textFile("README.md");
        JavaRDD<String> python = textFile.filter(line -> line.contains("python"));
        System.out.println(python.count());
        System.out.println(python.take(10));



    }







}
