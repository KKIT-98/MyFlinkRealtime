package com.kunan.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONObject;
import com.kunan.realtime.common.base.BaseAPP;
import com.kunan.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwdBaseLog extends BaseAPP {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务处理
        //1、进行ETL过滤不完整的数据
        //stream.print(); //先测试是否有数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);
        jsonObjStream.print();
        //2、进行新旧访客修复
        jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts"); //这里获取到的ts可能不为毫秒 可能要*1000  需要在etl中进行处理
                    }
                })).keyBy(new KeySelector<JSONObject, String>() {

            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid"); //设备ID
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    //{"actions":[{"action_id":"favor_add","item":"10","item_type":"sku_id","ts":1712021915706},{"action_id":"cart_add","item":"10","item_type":"sku_id","ts":1712021918706}],"common":{"ar":"21","ba":"vivo","ch":"vivo","is_new":"0","md":"vivo IQOO Z6x ","mid":"mid_402","os":"Android 13.0","sid":"e9768b74-09a2-4a64-8007-def860803b04","uid":"268","vc":"v2.1.134"},"displays":[{"item":"9","item_type":"sku_id","pos_id":4,"pos_seq":0},{"item":"33","item_type":"sku_id","pos_id":4,"pos_seq":1}],"page":{"during_time":6073,"from_pos_id":4,"from_pos_seq":3,"item":"10","item_type":"sku_id","last_page_id":"good_detail","page_id":"good_detail"},"ts":1712021913706}
                    //{"common":{"ar":"15","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone 14","mid":"mid_481","os":"iOS 13.3.1","sid":"ca532926-4cfe-4483-b456-0d75abffcd4c","vc":"v2.1.134"},"start":{"entry":"icon","loading_time":16837,"open_ad_id":15,"open_ad_ms":2672,"open_ad_skip_ms":58524},"ts":1712025051307}
                    JSONObject jsonObject = JSONObject.parseObject(value); //如果这里报错说明不是完整的json字符串
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (page != null || start != null) {
                        //防止keyby 和水位线 ts没有值或者mid没有值 导致程序直接报错
                        if (common != null && common.getString("mid") != null && ts != null) {
                            out.collect(jsonObject);
                        }

                    }
                } catch (Exception e) {
                    // e.printStackTrace();
                    System.out.println("过滤掉脏数据:" + value);
                }
            }
        });
    }
}
