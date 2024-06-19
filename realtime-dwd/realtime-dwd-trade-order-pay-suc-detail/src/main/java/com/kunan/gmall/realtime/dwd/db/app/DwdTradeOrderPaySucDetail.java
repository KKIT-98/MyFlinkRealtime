package com.kunan.gmall.realtime.dwd.db.app;

import com.kunan.realtime.common.base.BaseSQLAPP;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016,4,"dwd_trade_order_pay_suc_detail");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //核心业务逻辑
        //1、读取topic_db数据
        createTopicDb(groupId,tableEnv);


        //2、筛选支付成功的数据
        filterPaymentTable(tableEnv);
        //3、读取下单详情表数据
        createDwdOrderDetail(tableEnv, groupId);

        //4、创建base_dic字典表
        createBaseDic(tableEnv);

        //tableEnv.executeSql("select * from base_dic").print();
        //tableEnv.executeSql("select TO_TIMESTAMP_LTZ(ts * 1000,3) from order_detail").print();

        //5、使用interval join完成支付成功流和订单流数据关联
        intervalJoin(tableEnv);

        //6、使用lookup join完成维度退化
        Table resultTable = lookupJoin(tableEnv);

        //7、创建upsert kafka写出
        CreateUpSertKafkaSink(tableEnv);
        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();

    }

    public static void CreateUpSertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS +" (\n" +
                " id STRING\n" +
                ",order_id STRING\n" +
                ",user_id STRING\n" +
                ",payment_type_code STRING\n" +
                ",payment_type_name STRING\n" +
                ",payment_time STRING\n" +
                ",sku_id STRING\n" +
                ",province_id STRING\n" +
                ",activity_id STRING\n" +
                ",activity_rule_id STRING\n" +
                ",coupon_id STRING\n" +
                ",sku_name STRING\n" +
                ",order_price STRING\n" +
                ",sku_num STRING\n" +
                ",split_total_amount STRING\n" +
                ",split_activity_amount STRING\n" +
                ",split_coupon_amount STRING\n" +
                ",ts BIGINT\n" +
                ",PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
    }

    public static Table lookupJoin(StreamTableEnvironment tableEnv) {
        Table resultTable = tableEnv.sqlQuery("SELECT\n" +
                " a.id AS id\n" +
                ",a.order_id AS order_id\n" +
                ",a.user_id AS user_id\n" +
                ",a.payment_type AS payment_type_code\n" +
                ",b.info.dic_name AS payment_type_name\n" +
                ",a.payment_time AS payment_time\n" +
                ",a.sku_id AS sku_id\n" +
                ",a.province_id AS province_id\n" +
                ",a.activity_id AS activity_id\n" +
                ",a.activity_rule_id AS activity_rule_id\n" +
                ",a.coupon_id AS coupon_id\n" +
                ",a.sku_name AS sku_name\n" +
                ",a.order_price AS order_price\n" +
                ",a.sku_num AS sku_num\n" +
                ",a.split_total_amount AS split_total_amount\n" +
                ",a.split_activity_amount AS split_activity_amount\n" +
                ",a.split_coupon_amount AS split_coupon_amount\n" +
                ",a.ts AS ts\n" +
                "FROM pay_order a\n" +
                "LEFT JOIN base_dic FOR SYSTEM_TIME AS OF a.proc_time AS b\n" +
                "ON a.payment_type = b.rowkey");
        return resultTable;
    }

    public static void intervalJoin(StreamTableEnvironment tableEnv) {
        Table payOrderTable = tableEnv.sqlQuery("SELECT\n" +
                "b.id AS id\n" +
                ",a.order_id AS order_id\n" +
                ",a.user_id AS user_id\n" +
                ",a.payment_type AS payment_type\n" +
                ",a.callback_time AS payment_time\n" +
                ",b.sku_id AS sku_id\n" +
                ",b.province_id AS province_id\n" +
                ",b.activity_id AS activity_id\n" +
                ",b.activity_rule_id AS activity_rule_id\n" +
                ",b.coupon_id AS coupon_id\n" +
                ",b.sku_name AS sku_name\n" +
                ",b.order_price AS order_price\n" +
                ",b.sku_num AS sku_num\n" +
                ",b.split_total_amount AS split_total_amount\n" +
                ",b.split_activity_amount AS split_activity_amount\n" +
                ",b.split_coupon_amount AS split_coupon_amount\n" +
                ",a.ts AS ts\n" +
                ",a.proc_time AS proc_time\n" +
                "FROM payment a,order_detail b\n" +
                "where a.order_id = b.order_id\n" +
                "AND a.row_time BETWEEN b.row_time - INTERVAL '15' MINUTE AND b.row_time + INTERVAL '5' SECOND");

        tableEnv.createTemporaryView("pay_order",payOrderTable);
    }

    public static void createDwdOrderDetail(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table order_detail (\n" +
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
                ",row_time AS TO_TIMESTAMP_LTZ(ts * 1000,3) \n" +
                ",WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND \n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, groupId));
    }

    public static void filterPaymentTable(StreamTableEnvironment tableEnv) {
        Table paymentTable = tableEnv.sqlQuery("SELECT\n" +
                "`data`['id'] AS id\n" +
                ",`data`['order_id'] AS order_id\n" +
                ",`data`['user_id'] AS user_id\n" +
                ",`data`['payment_type'] AS payment_type\n" +
                ",`data`['total_amount'] AS total_amount\n" +
                ",`data`['callback_time'] AS callback_time\n" +
                ",ts\n" +
                ",row_time\n" +
                ",proc_time\n" +
                "FROM topic_db\n" +
                "WHERE  `database` = 'gmall'\n" +
                "AND `table` = 'payment_info'\n" +
                "AND `type` = 'update'\n" +
                "AND `old`['payment_status'] IS NOT NULL\n" +
                "AND `data`['payment_status'] = '1602'");
        tableEnv.createTemporaryView("payment",paymentTable);
    }
}
