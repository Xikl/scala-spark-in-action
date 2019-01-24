package com.ximo.spark.chap04;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

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


}
