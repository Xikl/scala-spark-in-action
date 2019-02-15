package com.ximo.spark.chap05;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author 朱文赵
 * @date 2019/2/15 14:13
 */
public class BasicLoadJson {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new Exception("Usage BasicLoadJson [sparkMaster] [jsoninput] [jsonoutput]");
        }
        String master = args[0];
        String fileName = args[1];
        String outfile = args[2];

        JavaSparkContext sc = new JavaSparkContext(
                master, "basicloadjson", System.getenv("SPARK_HOME"), System.getenv("JARS"));

        final JavaRDD<String> input = sc.textFile(fileName);
        // 先从文件中读取json数据 然后过滤
        final JavaRDD<Person> result = input.mapPartitions(new ParseJson()).filter(p -> p.lovesPanders);
        // 然后再将它写入到文本中 将rdd 变成字符串
        result.mapPartitions(people -> {
            List<String> text = new ArrayList<>();
            ObjectMapper objectMapper = new ObjectMapper();
            while (people.hasNext()) {
                final Person person = people.next();
                final String personJsonStr = objectMapper.writeValueAsString(person);
                text.add(personJsonStr);
            }
            return text.iterator();
        }).saveAsTextFile(outfile);



    }


    public static class Person implements Serializable {
        private String name;
        private Boolean lovesPanders;
    }

    public static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {

        @Override
        public Iterator<Person> call(Iterator<String> lines) throws Exception {
            List<Person> people = new ArrayList<>();
            ObjectMapper objectMapper = new ObjectMapper();
            while (lines.hasNext()) {
                String line = lines.next();
                try {
                    people.add(objectMapper.readValue(line, Person.class));
                } catch (Exception e) {
                    System.out.println(e.getCause());
                }
            }
            return people.iterator();
        }
    }



}
