package com.kunan.realtime.common.util;

import com.kunan.realtime.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaSourceSQL(String topicName,String GroupId){
       return "WITH (\n" +
               "      'connector' = 'kafka',\n" +
               "      'topic' = '" + topicName + "',\n" +
               "      'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
               "      'properties.group.id' = '" + GroupId + "',\n" +
               "      'scan.startup.mode' = 'earliest-offset',\n" +
               "      'format' = 'json'\n" +
                       ")";
    }

    public static String getKafkaTopicDb(String GroupId){
        return "CREATE TABLE topic_db (\n" +
                "`database` STRING,\n" +
                "`table` STRING,\n" +
                "`ts` bigint,\n" +
                "`data` map<STRING,STRING>,\n" +
                "`old` map<STRING,STRING>,\n" +
                "`type` STRING,\n" +
                "proc_time  AS PROCTIME(), \n" +
                "row_time  AS TO_TIMESTAMP_LTZ(ts * 1000,3), \n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND \n" +
                ")" + getKafkaSourceSQL(Constant.TOPIC_DB,GroupId);
    }
    //使用FlinkSql将数据写入kafka
    public static String getKafkaSinkSQL(String topicName){
        return "WITH (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = '" + topicName + "',\n" +
                "      'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "      'format' = 'json'\n" +
                ")";

    }
    //获取upsert kafka连接 创建表格语句最后一定要声明主键
    public static String getUpsertKafkaSQL(String topicName){

        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");";
    }
}
