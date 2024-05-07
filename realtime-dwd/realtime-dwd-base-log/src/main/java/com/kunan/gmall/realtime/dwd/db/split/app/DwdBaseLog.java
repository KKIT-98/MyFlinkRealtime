package com.kunan.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kunan.realtime.common.base.BaseAPP;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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
                    if (firstLoginDt != null && !firstLoginDt.equals(currDt)) {
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
        //新旧访客修复测试打印
        //isNewFixStream.print();


        // 3、拆分不同类型的用户行为日志
        // 1> 启动日志：启动信息  报错信息
        //      {"common":{"ar":"24","uid":"346","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 14","mid":"mid_290","vc":"v2.1.134","ba":"iPhone","sid":"d3d03ba0-ca95-48fc-b3e9-7dc7e7029a9d"},"start":{"entry":"icon","open_ad_skip_ms":0,"open_ad_ms":9159,"loading_time":12704,"open_ad_id":13},"ts":1712028584375}
        //      {"common":{"ar":"18","uid":"416","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 14","mid":"mid_433","vc":"v2.1.134","ba":"iPhone","sid":"ead2b1c3-b2a6-4a67-b8f7-788c4b545f9f"},"err":{"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.AppError.main(AppError.java:xxxxxx)","error_code":1089},"page":{"page_id":"cart","during_time":8329,"last_page_id":"good_detail"},"displays":[{"pos_seq":0,"item":"3","item_type":"sku_id","pos_id":5},{"pos_seq":1,"item":"15","item_type":"sku_id","pos_id":5},{"pos_seq":2,"item":"34","item_type":"sku_id","pos_id":5},{"pos_seq":3,"item":"15","item_type":"sku_id","pos_id":5}],"ts":1712019256967}
        // 2> 页面日志 : 页面信息 曝光信息 动作信息 报错信息
        //      {"common":{"ar":"24","os":"Android 12.0","ch":"xiaomi","is_new":"0","md":"Redmi k50","mid":"mid_327","vc":"v2.1.134","ba":"Redmi","sid":"7f2d7d98-863d-4189-9de0-8014d740623f"},"page":{"page_id":"activity1111","refer_id":"1","during_time":8675},"displays":[{"pos_seq":0,"item":"24","item_type":"sku_id","pos_id":8},{"pos_seq":1,"item":"6","item_type":"sku_id","pos_id":8},{"pos_seq":2,"item":"2","item_type":"sku_id","pos_id":8},{"pos_seq":3,"item":"7","item_type":"sku_id","pos_id":8},{"pos_seq":4,"item":"13","item_type":"sku_id","pos_id":8},{"pos_seq":5,"item":"9","item_type":"sku_id","pos_id":8},{"pos_seq":6,"item":"35","item_type":"sku_id","pos_id":8},{"pos_seq":7,"item":"24","item_type":"sku_id","pos_id":8},{"pos_seq":8,"item":"26","item_type":"sku_id","pos_id":8},{"pos_seq":9,"item":"30","item_type":"sku_id","pos_id":8},{"pos_seq":10,"item":"32","item_type":"sku_id","pos_id":8},{"pos_seq":11,"item":"2","item_type":"sku_id","pos_id":8},{"pos_seq":12,"item":"19","item_type":"sku_id","pos_id":8},{"pos_seq":0,"item":"13","item_type":"sku_id","pos_id":9},{"pos_seq":1,"item":"35","item_type":"sku_id","pos_id":9},{"pos_seq":2,"item":"6","item_type":"sku_id","pos_id":9},{"pos_seq":3,"item":"17","item_type":"sku_id","pos_id":9},{"pos_seq":4,"item":"7","item_type":"sku_id","pos_id":9},{"pos_seq":5,"item":"12","item_type":"sku_id","pos_id":9}],"ts":1712027870452}
        //      {"common":{"ar":"13","uid":"277","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 14 Plus","mid":"mid_94","vc":"v2.1.132","ba":"iPhone","sid":"3cc01eb0-d7c9-41bc-b780-85695bb27640"},"page":{"from_pos_seq":9,"page_id":"good_detail","item":"8","during_time":16322,"item_type":"sku_id","last_page_id":"good_list","from_pos_id":10},"displays":[{"pos_seq":0,"item":"25","item_type":"sku_id","pos_id":4},{"pos_seq":1,"item":"7","item_type":"sku_id","pos_id":4},{"pos_seq":2,"item":"11","item_type":"sku_id","pos_id":4},{"pos_seq":3,"item":"13","item_type":"sku_id","pos_id":4},{"pos_seq":4,"item":"2","item_type":"sku_id","pos_id":4},{"pos_seq":5,"item":"7","item_type":"sku_id","pos_id":4},{"pos_seq":6,"item":"29","item_type":"sku_id","pos_id":4},{"pos_seq":7,"item":"21","item_type":"sku_id","pos_id":4},{"pos_seq":8,"item":"16","item_type":"sku_id","pos_id":4}],"actions":[{"item":"8","action_id":"favor_add","item_type":"sku_id","ts":1712027431184},{"item":"8","action_id":"cart_add","item_type":"sku_id","ts":1712027434184}],"ts":1712027429184}
        //OutputTag<String> startTag = new OutputTag<String>("start"); //这样写会报错 泛型擦除
        //正确写法1
        //OutputTag<String> startTag = new OutputTag<String>("start"){};
        //正确写法2
        OutputTag<String> startTag = new OutputTag<String>("start", TypeInformation.of(String.class)); //启动
        OutputTag<String> errorTag = new OutputTag<String>("err", TypeInformation.of(String.class)); //报错
        OutputTag<String> displayTag = new OutputTag<String>("display", TypeInformation.of(String.class)); //曝光
        OutputTag<String> actionTag = new OutputTag<String>("action", TypeInformation.of(String.class)); //动作

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, errorTag, startTag, displayTag, actionTag);
        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errorStream = pageStream.getSideOutput(errorTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);
        //控制台打印测试
        pageStream.print("page=> ");
        startStream.print("start=> ");
        errorStream.print("error=> ");
        displayStream.print("display=> ");
        actionStream.print("action=> ");


    }

    private SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> errorTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> out) throws Exception {
                //核心逻辑 根据数据不同 拆分到不同的侧输出流
                JSONObject err = jsonObject.getJSONObject("err");
                if (err != null) {
                    //当前存在报错信息 单独将报错信息写出到报错侧输出流
                    context.output(errorTag, err.toJSONString());
                    //删除数据流中的err数据
                    jsonObject.remove("err");
                }
                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject start = jsonObject.getJSONObject("start");
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");

                if (start != null) {
                    //当前为启动日志 注意这里要输出 jsonObject 的完整日志信息
                    context.output(startTag, jsonObject.toJSONString());
                } else if (page != null) {
                    //当前为页面日志
                    JSONArray displays = jsonObject.getJSONArray("displays");//曝光存的数据形式数据
                    //不要使用循环直接掉 如 for(display displays) 会丢掉对象 使用fori循环
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //放入公共信息
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            //写出到侧输出流
                            context.output(displayTag, display.toJSONString());
                        }
                        //删除主数据流中的displays数据
                        jsonObject.remove("displays");
                    }


                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            //放入公共信息
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            context.output(actionTag, action.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }

                    //只保留page信息 写出到主流
                    out.collect(jsonObject.toJSONString());

                } else {
                    //留空
                }

            }
        });
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
