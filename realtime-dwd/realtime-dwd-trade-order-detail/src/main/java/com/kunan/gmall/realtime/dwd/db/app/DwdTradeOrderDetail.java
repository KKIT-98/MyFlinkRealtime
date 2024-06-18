package com.kunan.gmall.realtime.dwd.db.app;

import com.kunan.realtime.common.base.BaseSQLAPP;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10024,4,"dwd_trade_order_detail");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //在flinkSQL中使用join一定要设置状态存活时间5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));
        //核心业务逻辑
        //1、读取topic_db的数据
        createTopicDb(groupId,tableEnv);
        //2、筛选订单详情表数据
        fliterOd(tableEnv);
        //3、筛选订单信息表
        filterOi(tableEnv);
        //4、筛选订单详情活动关联表
        filterOda(tableEnv);
        //5、筛选订单详情优惠券关联表
        fliterOdc(tableEnv);
        //6、将四张表join合并
        Table joinTable = getJoinTable(tableEnv);
        //7、写出到Kafka
        // 使用了left join 会产生撤回流 此时如果需要将数据写出到kafka 不能使用一般的kafka sink必须使用upsert kafka
        //官方使用说明: https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/upsert-kafka/
        createUpsertKafkaSink(tableEnv);
        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();

    }

    public static void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table "+ Constant.TOPIC_DWD_TRADE_ORDER_DETAIL +"(\n" +
                "id STRING\n" +
                ",order_id STRING\n" +
                ",sku_id STRING\n" +
                ",province_id STRING\n" +
                ",user_id STRING\n" +
                ",activity_id STRING\n" +
                ",activity_rule_id STRING\n" +
                ",coupon_id STRING\n" +
                ",sku_name STRING\n" +
                ",order_price STRING\n" +
                ",sku_num STRING\n" +
                ",create_time STRING\n" +
                ",split_total_amount STRING\n" +
                ",split_activity_amount STRING\n" +
                ",split_coupon_amount STRING\n" +
                ",ts BIGINT\n" +
                ",PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }

    public static Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT\n" +
                "a.id id\n" +
                ",a.order_id order_id\n" +
                ",a.sku_id sku_id\n" +
                ",b.province_id  province_id\n" +
                ",b.user_id  user_id\n" +
                ",c.activity_id activity_id\n" +
                ",c.activity_rule_id activity_rule_id\n" +
                ",d.coupon_id coupon_id\n" +
                ",a.sku_name sku_name\n" +
                ",a.order_price order_price\n" +
                ",a.sku_num sku_num\n" +
                ",a.create_time create_time\n" +
                ",a.split_total_amount split_total_amount\n" +
                ",a.split_activity_amount split_activity_amount\n" +
                ",a.split_coupon_amount split_coupon_amount\n" +
                ",a.ts\n" +
                "FROM order_detail a\n" +
                "JOIN order_info b\n" +
                "ON a.order_id = b.id\n" +
                "LEFT JOIN order_detail_activity c\n" +
                "ON a.id = c.order_detail_id\n" +
                "LEFT JOIN order_detail_coupon d\n" +
                "on a.id = d.order_detail_id");
    }

    public static void fliterOdc(StreamTableEnvironment tableEnv) {
        Table odcTable = tableEnv.sqlQuery("SELECT\n" +
                "`data`['order_detail_id'] order_detail_id\n" +
                ",`data`['coupon_id'] coupon_id\n" +
                "FROM topic_db\n" +
                "WHERE  `database` = 'gmall'\n" +
                "AND `table` = 'order_detail_coupon'\n" +
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_coupon",odcTable);
    }

    public static void filterOda(StreamTableEnvironment tableEnv) {
        Table odaTable = tableEnv.sqlQuery("SELECT\n" +
                "`data`['order_detail_id'] order_detail_id\n" +
                ",`data`['activity_id'] activity_id\n" +
                ",`data`['activity_rule_id'] activity_rule_id\n" +
                "FROM topic_db\n" +
                "WHERE  `database` = 'gmall'\n" +
                "AND `table` = 'order_detail_activity'\n" +
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_activity",odaTable);
    }

    public static void filterOi(StreamTableEnvironment tableEnv) {
        Table oiTable = tableEnv.sqlQuery("SELECT\n" +
                "`data`['id'] id\n" +
                ",`data`['province_id'] province_id\n" +
                ",`data`['user_id'] user_id\n" +
                ",ts\n" +
                "FROM topic_db\n" +
                "WHERE  `database` = 'gmall'\n" +
                "AND `table` = 'order_info'\n" +
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("order_info",oiTable);
    }

    public static void fliterOd(StreamTableEnvironment tableEnv) {
        Table odtable = tableEnv.sqlQuery("SELECT\n" +
                "`data`['id'] id\n" +
                ",`data`['order_id'] order_id\n" +
                ",`data`['sku_id'] sku_id\n" +
                ",`data`['sku_name'] sku_name\n" +
                ",`data`['order_price'] order_price\n" +
                ",`data`['sku_num'] sku_num\n" +
                ",`data`['create_time'] create_time\n" +
                ",`data`['split_total_amount'] split_total_amount\n" +
                ",`data`['split_activity_amount'] split_activity_amount\n" +
                ",`data`['split_coupon_amount'] split_coupon_amount\n" +
                ",ts\n" +
                "FROM topic_db\n" +
                "WHERE  `database` = 'gmall'\n" +
                "AND `table` = 'order_detail'\n" +
                "AND `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail",odtable);
    }
}
