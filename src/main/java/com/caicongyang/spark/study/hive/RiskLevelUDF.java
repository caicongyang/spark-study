package com.caicongyang.spark.study.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive udf demo
 *
 * 使用方式：
 * 1./opt/spark/bin/spark-sql --master yarn --queue root.risk --keytab /home/caicongyang/keytab/caicongyang.keytab --principal caicongyang/xxx.hadoop@caicongyang.com --jars xxx.jar,xxx.jar  --conf "spark.yarn.appMasterEnv.JAVA_HOME=/usr/java/jdk1.8.0_74/" --conf "spark.executorEnv.JAVA_HOME=/usr/java/jdk1.8.0_74/"
 * 2.create temporary function riskLevel as 'com.caicongyang.spark.study.hive.RiskLevelUDF';
 * 3.select riskLevel(u.score) from t_user u where dt ='2019-01-01' limit 10
 *
 *
 */
public class RiskLevelUDF extends UDF {

    public String evaluate(String riskCode) {
        return String.valueOf(transform2RiskLevel(riskCode));
    }


    private static String transform2RiskLevel(String riskCode) {
        if (StringUtils.isNumeric(riskCode)) {
            return "error";
        }

        Integer score = Integer.valueOf(riskCode);


        if (score >= 50) {
            return "low";
        } else if (score > 50 && score < 85) {
            return "medium";
        } else if (score > 85) {
            return "high";
        }


        return "error";
    }

}