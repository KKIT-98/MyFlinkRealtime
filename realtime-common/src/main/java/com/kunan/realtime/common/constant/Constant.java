package com.kunan.realtime.common.constant;
/**
 * 常量类 公共(public) 静态(static) 不可修改(final)
 * */
public class Constant {
    /**kafka broker地址*/
    public static final String KAFKA_BROKERS = "192.168.31.102:9092,192.168.31.103:9092,192.168.31.104:9092";
    /**业务数据同步到kafka的主题*/
    public static final String TOPIC_DB = "topic_db";
    /**日志数据同步到kafka的主题*/
    public static final String TOPIC_LOG = "topic_log";
    /**MySQL安装节点地址*/
    public static final String MYSQL_HOST = "192.168.31.102";
    /**MySQL端口号*/
    public static final int MYSQL_PORT = 3306;
    /**MySQL用户*/
    public static final String MYSQL_USER_NAME = "root";
    /**MySQL密码*/
    public static final String MYSQL_PASSWORD = "000000";
    /**MySQL驱动类8.0*/
    //public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    /**MySQL驱动类5.7*/
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    /**MySQL驱动地址*/
    public static final String MYSQL_URL = "jdbc:mysql://192.168.31.102:3306?useSSL=false";
    /**配置表存储的库名*/
    public static final String PROCESS_DATABASE = "gmall2023_config";
    /**配置表名*/
    public static final String PROCESS_DIM_TABLE_NAME = "table_process_dim";
    /**HBase的连接地址*/
    public static final String HBASE_ZOOKEEPER_QUORUM = "192.168.31.102:2181,192.168.31.103:2181,192.168.31.104:2181";
    /**Hbase命名空间*/
    public static final String HBASE_NAMESPACE = "gmall";
    /**启动页面侧输出流发送到kafka的主题*/
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    /**报错信息侧输出流发送到kafka的主题*/
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    /**主流发送到kafka的主题*/
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    /**行动页面侧输出流发送到kafka的主题*/
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    /**曝光页面侧输出流发送到kafka的主题*/
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";


}
