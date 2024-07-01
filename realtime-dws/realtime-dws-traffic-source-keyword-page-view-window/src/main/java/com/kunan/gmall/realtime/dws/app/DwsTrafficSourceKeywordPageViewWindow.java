package com.kunan.gmall.realtime.dws.app;

import com.kunan.realtime.common.base.BaseSQLAPP;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021,4,"dws_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //核心业务逻辑
        //1、读取主流DWD页面主题数据
        tableEnv.executeSql("CREATE TABLE page_info(\n" +
                "`common` map<STRING,STRING>\n" +
                ",`page` map<STRING,STRING>\n" +
                ",`ts` bigint\n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE,groupId));



        //2、筛选出关键字keywords
        tableEnv.executeSql("SELECT\n" +
                "page['item']  keyword\n" +
                ",`ts`\n" +
                "FROM page_info\n" +
                "WHERE page['last_page_id'] = 'search'\n" +
                "AND page['item_type'] = 'keyword'\n" +
                "AND page['item'] IS NOT NULL").print();

        //3、创建自定义UDTF分词函数 并注册


        //4、调用分词函数对keywords进行拆分


        //5、对keyword进行分组开窗聚合


        //6、写出到doris
    }
}
