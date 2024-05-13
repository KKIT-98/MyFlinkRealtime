import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Test03 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //状态存活时间
       // tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10L));
        //读取kafka
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "`database` STRING,\n" +
                "`table` STRING,\n" +
                "`ts` bigint,\n" +
                "`data` map<STRING,STRING>,\n" +
                "`old` map<STRING,STRING>,\n" +
                "`type` STRING,\n" +
                "proc_time  AS PROCTIME() \n" +
                ") WITH (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = 'topic_db',\n" +
                "      'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "      'properties.group.id' = 'testGroup',\n" +
                "      'scan.startup.mode' = 'earliest-offset',\n" +
                "      'format' = 'json'\n" +
                ")");
        // 打印1
/*
        Table table = tableEnv.sqlQuery("SELECT \n" +
                "    * \n" +
                "FROM topic_db \n" +
                "WHERE `database` = 'gmall' \n" +
                "AND `table` = 'comment_info'");
        table.execute().print();
*/

        // 打印2
       /*  tableEnv.executeSql("SELECT \n" +
                "    * \n" +
                "FROM topic_db \n" +
                "WHERE `database` = 'gmall' \n" +
                "AND `table` = 'comment_info'").print();*/


         //直接读取mysql
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "dic_code STRING,\n" +
                "dic_name STRING,\n" +
                "parent_code STRING,\n" +
                "PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.31.102:3306/gmall',\n" +
                "    'table-name' = 'base_dic',\n" +
                "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "     'password' = '000000'\n" +
                ")");

       // tableEnv.executeSql("SELECT * FROM base_dic").print();


        //过滤出comment_info的对应信息
        Table commentInfo = tableEnv.sqlQuery("SELECT\n" +
                "`data`['id'] AS id\n" +
                ",`data`['user_id'] as user_id\n" +
                ",`data`['nick_name'] as nick_name\n" +
                ",`data`['head_img'] as head_img\n" +
                ",`data`['sku_id'] as sku_id\n" +
                ",`data`['spu_id'] as spu_id\n" +
                ",`data`['order_id'] as order_id\n" +
                ",`data`['appraise'] as appraise \n" +
                ",`data`['comment_txt'] as comment_txt\n" +
                ",`data`['create_time'] as create_time\n" +
                ",`data`['operate_time'] as operate_time\n" +
                ",proc_time\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'comment_info'\n" +
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        //使用join
     /*   tableEnv.executeSql("SELECT\n" +
                        "a.id\n" +
                        ",a.user_id\n" +
                        ",a.nick_name\n" +
                        ",a.head_img\n" +
                        ",a.sku_id\n" +
                        ",a.spu_id\n" +
                        ",a.order_id\n" +
                        ",a.appraise AS appraise_code\n" +
                        ",b.dic_name as appraise_name\n" +
                        ",a.comment_txt\n" +
                        ",a.create_time\n" +
                        ",a.operate_time\n" +
                        "FROM comment_info a\n" +
                        "JOIN base_dic b\n" +
                        "ON a.appraise = b.dic_code")
                .print();*/

        //使用时态关联 Event Time Temporal Join
        tableEnv.executeSql("SELECT\n" +
                        "a.id\n" +
                        ",a.user_id\n" +
                        ",a.nick_name\n" +
                        ",a.head_img\n" +
                        ",a.sku_id\n" +
                        ",a.spu_id\n" +
                        ",a.order_id\n" +
                        ",a.appraise AS appraise_code\n" +
                        ",b.dic_name as appraise_name\n" +
                        ",a.comment_txt\n" +
                        ",a.create_time\n" +
                        ",a.operate_time\n" +
                        "FROM comment_info a\n" +
                        "JOIN base_dic FOR SYSTEM_TIME AS OF a.proc_time b\n" +
                        "ON a.appraise = b.dic_code")
                .print();

    }
}
