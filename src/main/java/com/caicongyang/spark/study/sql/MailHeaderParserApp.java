package com.caicongyang.spark.study.sql;

import com.caicongyang.spark.study.utils.MailParserUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 解析邮件头 spark 任务
 *
 * @author caicongyang
 * @version id: MailHeaderParserApp, v 0.1 18/3/6 下午5:37 caicongyang1 Exp $$
 */
public class MailHeaderParserApp {

    public static final Logger LOGGER = LoggerFactory.getLogger(MailHeaderParserApp.class);


    public static void main(String[] args) {

        /**
         *  SparkSession实质上是SQLContext和HiveContext的组合（未来可能还会加上StreamingContext），
         *  所以在SQLContext和HiveContext上可用的API在SparkSession上同样是可以使用的。
         *  SparkSession内部封装了sparkContext，所以计算实际上是由sparkContext完成的。
         */
        SparkSession spark = SparkSession.builder().appName("MailHeaderParserApp").master("yarn").enableHiveSupport()
                .config("hive.exec.dynamic.partition", "true")
                .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate();
        SQLContext sqlContext = spark.sqlContext();

        UDFRegistration udfRegistration = sqlContext.udf();
        udfRegistration.register("mailParser", new UDF1<String, String>() {
            @Override
            public String call(String mailHeader) throws Exception {
                return MailParserUtil.parse(mailHeader);
            }
        }, DataTypes.StringType);

        String applyDay = args[0];

        LOGGER.info("args applyDay is {}", applyDay);

        if (StringUtils.isNotBlank(applyDay)) {
            spark.sql("use vdm_risk");
            //创建表
            String createTable = "create table if not exists vdm_risk.mail_parser_info(mail_id STRING,mail_header STRING) PARTITIONED BY (dt STRING)";
            spark.sql(createTable);
            String querySql = "insert overwrite table vdm_risk.mail_parser_info partition(dt) select mi.mail_id, mailParser(mi.mail_header) as mail_header, substr(mi.gmt51_create_datetime,1,10) as dt from risk_access_views.o_pgsql_riskbill_public_v_mail_info  as mi where substr(mi.gmt51_create_datetime,1,10) <'${date}'";

            querySql = querySql.replaceAll("\\$\\{date}", applyDay);
            LOGGER.info(querySql);
            spark.sql(querySql);
        } else {
            throw new RuntimeException("请传入日期参数");
        }
        spark.stop();

    }


}
