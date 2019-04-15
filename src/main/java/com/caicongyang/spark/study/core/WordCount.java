package com.caicongyang.spark.study.core;


import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 单词统计
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        String inputFile = "file:///Users/caicongyang/spark-input.txt";
        String outputFile = "file:///Users/caicongyang/spark-out.txt";
        String outputUri = "/Users/caicongyang/spark-out.txt";

        // set master 的计划中方式 https://blog.csdn.net/u013013225/article/details/80566334
        SparkConf conf = new SparkConf().setAppName("caicongyang-wordCount").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读本地文件
        JavaRDD<String> input = sc.textFile(inputFile);


        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(",")).iterator();
            }
        });


        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String x) throws Exception {
                return new Tuple2<String, Integer>(x, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });


        //写本地文件，如果存在就删除
        File file = new File(outputUri);
        if (file.isFile()) {
            file.delete();
        } else {
            FileUtils.deleteDirectory(file);
        }
        counts.saveAsTextFile(outputFile);

        sc.stop();
    }


}
