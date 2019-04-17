package com.caicongyang.spark.study.hive;

import com.caicongyang.spark.study.User;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * json 转成 临时hive 表
 */
public class JsonApp {

    private static String inputFile = "file:///Users/caicongyang/spark/json2.json";


    public static void main(String[] args) {


        SparkSession spark = SparkSession.builder().appName("caicongyang-hiveJsonApp").master("local[2]").enableHiveSupport().getOrCreate();


        SQLContext sqlContext = spark.sqlContext();


        //{"user":{"name":"Jack","country":"China"},"text":"I like Chinese"}
        //{"user":{"name":"Tom","country":"USA"},"text":"I like USA"}
        Dataset<Row> rowDataset = sqlContext.read().json(inputFile);


        rowDataset.createOrReplaceTempView("T_User");
        Dataset<Row> dataset = sqlContext.sql("select user.name ,user.text FROM T_User as user");




        // 生成schema
//        List<StructField> fields = new ArrayList<>();
//        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("text", DataTypes.StringType, true));
//        StructType schema = DataTypes.createStructType(fields);

        List<User> userList = dataset.as(Encoders.bean(User.class))
                .collectAsList();

        System.out.println("partitions size:"+dataset.rdd().getPartitions().length);

        System.out.println(userList.get(0).toString());
        System.out.println(userList.get(1).toString());




    }
}
