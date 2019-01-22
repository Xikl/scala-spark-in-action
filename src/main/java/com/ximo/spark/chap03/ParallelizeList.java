package com.ximo.spark.chap03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author 朱文赵
 * @date 2019/1/21 15:36
 */
public class ParallelizeList {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("scala-spark-in-action");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.parallelize(Arrays.asList("hello world", "word", "python"));
        rdd.flatMap(word -> Arrays.asList(word.split(" ")).iterator());
        // 抽样 第一个参数为 是否可以多次 第二个参数为 概率
        rdd.sample(false, 0.5);

        // 累加操作
        JavaRDD<Integer> rdd2 = sparkContext.parallelize(Arrays.asList(1, 3, 4, 2));

        rdd2.reduce((a, b) -> a + b);
        // 标准java接口中的reduce操作 提供默认值
        rdd2.fold(0, (a, b) -> a + b);

    }

    private static void testActionCountAndTake(JavaSparkContext sparkContext) {
        JavaRDD<String> textFile = sparkContext.textFile("README.md");
        JavaRDD<String> python = textFile.filter(line -> line.contains("python"));
        System.out.println(python.count());
        System.out.println(python.take(10));
    }

    public static void testMap(JavaSparkContext sparkContext) {
        final JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 4));
        rdd.map(x -> x * x).collect();
    }








}
