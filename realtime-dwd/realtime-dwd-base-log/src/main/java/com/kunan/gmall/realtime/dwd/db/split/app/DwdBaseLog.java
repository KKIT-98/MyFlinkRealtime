package com.kunan.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONObject;
import com.kunan.realtime.common.base.BaseAPP;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
        //jsonObjStream.print();
        //2、进行新旧访客修复

        KeyedStream<JSONObject, String> keyedStream = KeyByWaterMark(jsonObjStream);
        //参数说明String：key  JSONObject：当前的数据 输出的数据结果类型
        SingleOutputStreamOperator<JSONObject> isNewFixStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //创建状态 状态是先创建再存储 再使用
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt", String.class));
            }

            @Override
            public void processElement(JSONObject value, Context context, Collector<JSONObject> out) throws Exception {
                // 1.获取当前数据的is_new字段
                JSONObject common = value.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = value.getLong("ts");
                String currDt = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
                    //判断当前状态情况
                    if (firstLoginDt != null && firstLoginDt.equals(currDt)) {
                        //如果状态不为空 且日期不是今天 说明当前数据错误 不是新访客 伪装新访客
                        common.put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        //如果状态为空 当前为新访客 将时间存入状态中
                        firstLoginDtState.update(currDt);
                    } else {
                        // 当前数据是同一天新访客重复登录 不进行处理
                    }
                } else if ("0".equals(isNew)) { //is_new=0
                    if (firstLoginDt == null) {
                        // 当前数据是老用户 flink实时数仓未记录该访客 需补充该访客信息
                        // 把访客首次登录日期补充一个值 今天以前的任意一天(这里使用昨天日期)
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        //正常情况无需处理
                    }
                } else {
                    // 当前数据 is_new 不为0 也不为 1 是错误数据
                }
                out.collect(value);
            }

        });
        isNewFixStream.print();

    }

    public KeyedStream<JSONObject, String> KeyByWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
/*        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
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
        });*/

        //测试使用
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
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
                     //e.printStackTrace();
                    System.out.println("过滤掉脏数据:" + value);
                }
            }
        });
    }
}
