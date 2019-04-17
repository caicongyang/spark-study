package com.caicongyang.spark.study.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * spark 算子练习
 */
public class OpeatorApp {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("caicongyang-OpeatorAppApp").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list1 = Arrays.asList("spark", "hadoop", "hbase","spark","java");

        JavaRDD<String> sourceRdd = sc.parallelize(list1);
        sourceRdd.distinct(); //去重； 开销大 会进行shffle

        List<String> collect = sourceRdd.collect(); // 取出所有的rdd 操作结果，仅仅可以用测试，或者结果数据较小的时候
        System.out.println("collect:"+StringUtils.join(collect,","));

        System.out.println("take:"+sourceRdd.take(1)); //取出第0个



        List<String> list2 = Arrays.asList("java", "scala", "go","c++");



        JavaRDD<String> lanRdd = sc.parallelize(list2);
        // reduce 操作
        String reduce = lanRdd.reduce((Function2<String, String, String>) (s1, s2) -> s1 +","+ s2);


        System.out.println(reduce);



        // reduceByKey
        List<String> list3 = Arrays.asList("java", "I like scala", "go","c++");

        JavaRDD<String> rdd3 = sc.parallelize(list3);

        //对字符串进行键值对处理，讲字符的第一个字符串作为key ，本身作为value  的键值对
        JavaPairRDD<String, String> javaPairRDD = rdd3.mapToPair(new PairFunction<String, String, String>() {

            @Override
            public Tuple2<String, String> call(String x) throws Exception {
                return new Tuple2<String, String>(x.split(" ")[0], x);
            }
        });

        JavaRDD<String> keys = javaPairRDD.keys();

        System.out.println("keys:"+StringUtils.join(keys.collect(),","));
        System.out.println("take2:"+javaPairRDD.take(1).toString());


        System.out.println("lookup:"+javaPairRDD.lookup("I"));



        sc.stop();

    }
}
