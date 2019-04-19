package com.caicongyang.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.*;

/**
 * 广播变量
 */
public class BroadCastApp {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("caicongyang-BroadCastApp").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("0571-54777365", "010-33681234", "0591-24777981");


        Map<String, String> config = new HashMap<>();
        config.put("0591", "福建");
        config.put("0571", "浙江");
        config.put("010", "北京");


        JavaRDD<String> javaRDD = sc.parallelize(data);

        final Broadcast<Map<String, String>> broadcast = sc.broadcast(config);
        JavaRDD<String> result = javaRDD.map(new Function<String, String>() {
            Map<String, String> map = broadcast.value();

            @Override
            public String call(String x) throws Exception {

                Set<String> keys = map.keySet();
                String pre = x.split("-")[0];
                if (keys.contains(pre)) {
                    return x + " " + map.get(pre);
                } else {
                    return x;
                }

            }
        });
        System.out.println(result.collect());

    }

}
