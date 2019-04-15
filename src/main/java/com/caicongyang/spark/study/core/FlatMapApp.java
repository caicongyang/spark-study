package com.caicongyang.spark.study.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

public class FlatMapApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("caicongyang-FlatMapApp").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("coffee panda", "happy panda", "happiest tiger party"));
        JavaRDD<String> result1 = rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.contains("panda")) {
                    return true;
                }
                return false;
            }
        }).map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s + "-";
            }
        });

        System.out.println("result1:"+StringUtils.join(result1.collect(),","));


        JavaRDD<String> result2 = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String key) throws Exception {
                return Arrays.asList(key.split(" ")).iterator();
            }
        });

        System.out.println("result2:"+StringUtils.join(result2.collect(),","));



        //结束进程
        sc.stop();
    }
}
