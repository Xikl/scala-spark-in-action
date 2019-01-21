package com.ximo.spark.chap02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author 朱文赵
 * @date 2019/1/17 16:25
 */
public class JavaSparkExample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("scala-spark-in-action");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> input = sparkContext.textFile("README.md");

//        input.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" ")).iterator();
//            }
//        });

        JavaRDD<String> words = input
                .filter(line -> line.contains("python"))
                .flatMap(line -> Arrays.stream(line.split(" ")).iterator());

        // pdf中自带的一种写法 统计词频
        JavaPairRDD<String, Integer> counts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((x, y) -> x + y);

        Map<String, Integer> wordCountMap = counts.collectAsMap();
        // 保存为文件
        counts.saveAsTextFile("anywhere");
    }



}
