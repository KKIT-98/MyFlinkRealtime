package com.kunan.gmall.realtime.dwd.db.app;

import com.kunan.realtime.common.base.BaseSQLAPP;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,"dwd_interaction_comment_info");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env,String GroupId) {
        //核心逻辑
        //1、读取topic_db
        createTopicDb(GroupId,tableEnv);
        
        //2、读取base_dic
        createBaseDic(tableEnv);
        
        //3、清洗topic_db 筛选出评论信息表新增的数据
        filterCommentInfo(tableEnv);
        
        //4、使用lookUp join 完成维度退化
        Table joinTable = lookUpJoin(tableEnv);
        //bug: 输出为空 原因定位 hbase维度表gmall:dim_base_dic 没有数据 ，排查发现没有将mysql维度表数据同步到kafka
        
        //5、创建KafkaSink对应的表
        CreateKafkaSinkTable(tableEnv);
        //6、写出到kafka对应的主题
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();
       //执行报错:  No operators defined in streaming topology. Cannot execute.
        // 解决： 将构建FlinkSQL入口通用方法中的  env.execute(); 去掉即可
    }

    public void CreateKafkaSinkTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + "(" +
                "id STRING\n" +
                ",user_id STRING\n" +
                ",nick_name STRING\n" +
                ",sku_id STRING\n" +
                ",spu_id STRING\n" +
                ",order_id STRING\n" +
                ",appraise_code STRING\n" +
                ",appraise_name STRING\n" +
                ",comment_txt STRING\n" +
                ",create_time STRING\n" +
                ",operate_time STRING" +
                ")"
        + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
    }

    public Table lookUpJoin(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT\n" +
                "a.id\n" +
                ",a.user_id\n" +
                ",a.nick_name\n" +
                ",a.sku_id\n" +
                ",a.spu_id\n" +
                ",a.order_id\n" +
                ",a.appraise AS appraise_code\n" +
                ",info.dic_name as appraise_name\n" +
                ",a.comment_txt\n" +
                ",a.create_time\n" +
                ",a.operate_time\n" +
                "FROM comment_info a\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF a.proc_time b\n" +
                "ON a.appraise = b.rowkey");
    }

    public void filterCommentInfo(StreamTableEnvironment tableEnv) {
        Table commentInfo = tableEnv.sqlQuery("SELECT\n" +
                "`data`['id'] AS id\n" +
                ",`data`['user_id'] as user_id\n" +
                ",`data`['nick_name'] as nick_name\n" +
                ",`data`['head_img'] as head_img\n" +
                ",`data`['sku_id'] as sku_id\n" +
                ",`data`['spu_id'] as spu_id\n" +
                ",`data`['order_id'] as order_id\n" +
                ",`data`['appraise'] as appraise\n" +
                ",`data`['comment_txt'] as comment_txt\n" +
                ",`data`['create_time'] as create_time\n" +
                ",`data`['operate_time'] as operate_time\n" +
                ",proc_time\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'comment_info'\n" +
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("comment_info",commentInfo);
    }
}
