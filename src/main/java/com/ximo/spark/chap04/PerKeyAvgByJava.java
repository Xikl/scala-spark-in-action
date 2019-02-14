package com.ximo.spark.chap04;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author 朱文赵
 * @date 2019/1/24 15:01
 */
public class PerKeyAvgByJava {


    public static void main(String[] args) {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }

        JavaSparkContext sc = new JavaSparkContext(
                master, "PerKeyAvg", System.getenv("SPARK_HOME"), System.getenv("JARS"));

        List<Tuple2<String, Integer>> input = new LinkedList<>();
        input.add(new Tuple2<>("coffee", 1));
        input.add(new Tuple2<>("coffee", 2));
        input.add(new Tuple2<>("pandas", 3));

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);

        // 创建算子
        Function<Integer, AvgCount> createAcc = value -> new AvgCount(value, 1);

        // 求和 计数
        Function2<AvgCount, Integer, AvgCount> addAndCount = (avgCount, value) -> {
            avgCount.setTotal(avgCount.getTotal() + value);
            avgCount.setNum(avgCount.getNum() + 1);
            return avgCount;
        };

        // 合并两个的值
        Function2<AvgCount, AvgCount, AvgCount> combine = (prev, next) -> {
            prev.setTotal(prev.getTotal() + next.getTotal());
            prev.setNum(prev.getNum() + next.getNum());
            return prev;
        };

        rdd.combineByKey(createAcc, addAndCount, combine)
            .collectAsMap().forEach((key, value) -> {
            System.out.println(key + " : " + value.getTotal() + "," + value.getNum());
        });


    }

    /**
     *  {(1, 2), (3, 4), (3, 6)}
     *
     * java sortByKey
     * countByKey 对key进行计数 {(1, 1), (3, 2)}
     * collectAsMap 收集为map 无论是java 还是scala 中的map key 都不会重复 默认后者会覆盖前者
     * lookup(key) 返回给定的健的对应的value值
     *
     * @param sc {@link JavaSparkContext}
     */
    private void sortByKey(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> input = new LinkedList<>();
        input.add(new Tuple2<>(111, "python"));
        input.add(new Tuple2<>(22222, "scala"));

        final Map<Integer, String> integerStringMap =
                sc.parallelizePairs(input).sortByKey(Comparator.comparing(Object::toString)).collectAsMap();
        integerStringMap.forEach((key, value) -> System.out.println(key + "" + value));
    }

}
