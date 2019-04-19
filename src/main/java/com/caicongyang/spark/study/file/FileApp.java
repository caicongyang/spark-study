package com.caicongyang.spark.study.file;

import com.caicongyang.spark.study.Person;
import com.caicongyang.spark.study.utils.GsonUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.ArrayList;
import java.util.Iterator;

public class FileApp {


    public static void main(String[] args) throws Exception {
        String inputFile = "file:///Users/caicongyang/spark/person.json";

        SparkConf conf = new SparkConf().setAppName("caicongyang-FileApp").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读本地文件
        JavaRDD<String> input = sc.textFile(inputFile);

        // 异常累加器
        LongAccumulator exceptionCounter = sc.sc().longAccumulator("exceptionCounter");


        // 按行读取 json 字符串解析成Person对象
        JavaRDD<Person> personJavaRDD = input.mapPartitions(new FlatMapFunction<Iterator<String>, Person>() {
            @Override
            public Iterator<Person> call(Iterator<String> iterator) throws Exception {
                ArrayList<Person> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    try {
                        list.add(GsonUtils.fromJson(next, Person.class));
                    } catch (Exception e) {
                        exceptionCounter.add(1L);
                    }
                }
                return list.iterator();
            }
        });


        System.out.println(personJavaRDD.count());

        //输出累加器
        System.out.println(exceptionCounter.count());


        // 为了展示ui页面
        Thread.sleep(1000 * 60 * 60);

    }
}
