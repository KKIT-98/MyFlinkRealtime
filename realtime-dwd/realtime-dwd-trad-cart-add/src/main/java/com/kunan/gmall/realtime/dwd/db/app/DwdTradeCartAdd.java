package com.kunan.gmall.realtime.dwd.db.app;

import com.kunan.realtime.common.base.BaseSQLAPP;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *筛选加购事务表明细数据且发送到kafka
 * **/
public class DwdTradeCartAdd extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4,"dwd_trade_cart_add");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        //核心业务编写
        //1、读取topic_db 数据
        createTopicDb(ckAndGroupId,tableEnv);

        //2、筛选加购数据
        Table cartAddTable = filterCartAdd(tableEnv);

        //3、创建kafkasink输出映射
        CreateKafkaSinkTable(tableEnv);
        
        //4、写出筛选的数据到对应的kafka主题
        cartAddTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();
        
    }

    public TableResult CreateKafkaSinkTable(StreamTableEnvironment tableEnv) {
        return tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                "id STRING\n" +
                ",user_id STRING\n" +
                ",sku_id STRING\n" +
                ",cart_price STRING\n" +
                ",sku_num STRING\n" +
                ",sku_name STRING\n" +
                ",is_checked STRING\n" +
                ",create_time STRING\n" +
                ",operate_time STRING\n" +
                ",is_ordered STRING\n" +
                ",order_time STRING\n" +
                ",ts BIGINT\n" +
                ")"
                + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    }

    public Table filterCartAdd(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT\n" +
                "`data`['id']   AS id\n" +
                ",`data`['user_id'] AS user_id\n" +
                ",`data`['sku_id'] AS sku_id\n" +
                ",`data`['cart_price'] AS cart_price\n" +
                ",IF(`type` = 'insert',`data`['sku_num'],CAST((CAST(`data`['sku_num'] AS BIGINT) - CAST(`old`['sku_num'] AS BIGINT)) AS STRING))  AS sku_num\n" +
                ",`data`['sku_name'] AS sku_name\n" +
                ",`data`['is_checked']  AS is_checked\n" +
                ",`data`['create_time']  AS create_time\n" +
                ",`data`['operate_time']  AS operate_time\n" +
                ",`data`['is_ordered']  AS is_ordered\n" +
                ",`data`['order_time']  AS order_time\n" +
                ",ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'cart_info'\n" +
                "AND (`type` = 'insert' \n" +
                "         OR \n" +
                "     (`type` = 'update' AND `old`['sku_num'] IS NOT NULL AND CAST(`data`['sku_num'] AS BIGINT) > CAST(`old`['sku_num'] AS BIGINT))\n" +
                "    )");
    }
}
