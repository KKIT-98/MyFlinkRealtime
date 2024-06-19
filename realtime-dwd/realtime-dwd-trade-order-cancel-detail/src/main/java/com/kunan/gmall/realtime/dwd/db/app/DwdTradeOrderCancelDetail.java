package com.kunan.gmall.realtime.dwd.db.app;

import com.kunan.realtime.common.base.BaseSQLAPP;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 交易域取消订单事实
 */
public class DwdTradeOrderCancelDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(10015,4,"dwd_trade_order_cancel_detail");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //设置状态存活时间5s 取消订单时只有订单表的状态发生变化，要和其它三张表关联，就需要考虑取消订单的延迟，通常在支付前均可取消订单，因此将 ttl 设置为 15min + 5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5)); //30min + 5s
        //1、读取topic_db数据
        createTopicDb(groupId,tableEnv);
        //2、从kafka读取 dwd 层下单事务事实表数据
        readDwdOD(tableEnv);
        
        //3、从 topic_db 过滤出订单取消数据
        filterOc(tableEnv);
        //测试输出
       // tableEnv.executeSql("select * from order_cancel").print();
       // tableEnv.executeSql("select * from " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).print();

        //4、订单取消表和下单表进行 join
        Table joinResulttable = getJoinResulttable(tableEnv);
        // tableEnv.createTemporaryView("dwd_trade_order_cancel_detail",joinResulttable);
       // tableEnv.executeSql("select * from dwd_trade_order_cancel_detail").print();
        //5、将join后的交易域取消订单事实表写出到kafka
        SinktoKafkaTable(tableEnv);
        joinResulttable.executeInsert("dwd_trade_order_cancel_detail");




    }

    public static void SinktoKafkaTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table dwd_trade_order_cancel_detail(\n" +
                " id STRING\n" +
                ",order_id STRING\n" +
                ",sku_id STRING\n" +
                ",province_id STRING\n" +
                ",user_id STRING\n" +
                ",activity_id STRING\n" +
                ",activity_rule_id STRING\n" +
                ",coupon_id STRING\n" +
                ",order_cancel_date_id STRING\n" +
                ",operate_time STRING\n" +
                ",sku_name STRING\n" +
                ",order_price STRING\n" +
                ",sku_num STRING\n" +
                ",create_time STRING\n" +
                ",split_total_amount STRING\n" +
                ",split_activity_amount STRING\n" +
                ",split_coupon_amount STRING\n" +
                ",ts BIGINT\n" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
    }

    public static Table getJoinResulttable(StreamTableEnvironment tableEnv) {
        Table joinResulttable = tableEnv.sqlQuery("SELECT\n" +
                " a.id as id\n" +
                ",a.order_id as order_id\n" +
                ",a.sku_id as sku_id\n" +
                ",a.province_id as province_id\n" +
                ",a.user_id as user_id\n" +
                ",a.activity_id as activity_id\n" +
                ",a.activity_rule_id as activity_rule_id\n" +
                ",a.coupon_id as coupon_id\n" +
                ",date_format(b.operate_time, 'yyyy-MM-dd') as order_cancel_date_id\n" +
                ",b.operate_time as operate_time\n" +
                ",a.sku_name as sku_name\n" +
                ",a.order_price as order_price\n" +
                ",a.sku_num as sku_num\n" +
                ",a.create_time as create_time\n" +
                ",a.split_total_amount as split_total_amount\n" +
                ",a.split_activity_amount as split_activity_amount\n" +
                ",a.split_coupon_amount as split_coupon_amount\n" +
                ",b.ts as ts\n" +
                "FROM "+ Constant.TOPIC_DWD_TRADE_ORDER_DETAIL +" a\n" +
                "JOIN order_cancel b\n" +
                "ON a.order_id = b.id");
        return joinResulttable;
    }

    public static void filterOc(StreamTableEnvironment tableEnv) {
        Table ocTable = tableEnv.sqlQuery("SELECT\n" +
                "`data`['id'] id\n" +
                ",`data`['operate_time'] operate_time\n" +
                ",ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_info'\n" +
                "AND `type` = 'update'\n" +
                "AND `old`['order_status'] = '1001'\n" +
                "AND `data`['order_status'] = '1003'");
        tableEnv.createTemporaryView("order_cancel",ocTable);
    }

    public static void readDwdOD(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table "+ Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(\n" +
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
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,"dwd_trade_order_cancel_detail"));
    }
}
