package com.ximo.javaspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
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
        input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaRDD<String> words = input.flatMap(line -> Arrays.stream(line.split(" ")).iterator());

        // pdf中自带的一种写法 统计词频
        JavaPairRDD<String, Integer> counts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((x, y) -> x + y);

        // 按照自己的理解编写 统计词频
        long myWordCount = words.groupBy((Function<String, String>) v1 -> v1).count();
        // 词频统计 转化为map
        Map<String, Integer> wordCountMap = counts.collectAsMap();
        // 保存为文件
        counts.saveAsTextFile("anywhere");
    }



}
