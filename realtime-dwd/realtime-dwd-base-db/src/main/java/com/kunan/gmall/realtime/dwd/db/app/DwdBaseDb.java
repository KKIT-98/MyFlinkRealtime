package com.kunan.gmall.realtime.dwd.db.app;

import com.alibaba.fastjson.JSONObject;
import com.kunan.realtime.common.base.BaseAPP;
import com.kunan.realtime.common.bean.TableProcessDwd;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.FlinkSinkUtil;
import com.kunan.realtime.common.util.FlinkSourceUtil;
import com.kunan.realtime.common.util.JDBCUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DwdBaseDb extends BaseAPP {
    public static void main(String[] args) {
        new DwdBaseDb().start(10019,4,"dwd_base_db", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //核心业务逻辑
        //1、读取topic_db数据
       //  kafkaSource.print();

        //2、清洗过滤和转换
        SingleOutputStreamOperator<JSONObject> jsonObjStream = flatMapToJsonObj(kafkaSource);
        //3、使用FlinkCdc读取配置表数据
        DataStreamSource<String> tableProcessDwd = env.fromSource(FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWD_TABLE_NAME), WatermarkStrategy.noWatermarks(), "table_process_dwd").setParallelism(1);
       // tableProcessDwd.print();

        //4、转换数据格式 根据op判断数据对应位置 取出数据
        //{"before":null,"after":{"source_table":"coupon_use","source_type":"insert","sink_table":"dwd_tool_coupon_get","sink_columns":"id,coupon_id,user_id,get_time,coupon_status"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2023_config","sequence":null,"table":"table_process_dwd","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1718867323318,"transaction":null}
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = tableProcessDwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String value, Collector<TableProcessDwd> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDwd processDwd;
                    if ("d".equals(op)) {
                        processDwd = jsonObject.getObject("before", TableProcessDwd.class);
                    } else {
                        processDwd = jsonObject.getObject("after", TableProcessDwd.class);
                    }
                    processDwd.setOp(op);
                    out.collect(processDwd);
                } catch (Exception e) {
                    System.out.println("捕获脏数据" + value);
                }
            }
        }).setParallelism(1);
        MapStateDescriptor<String, TableProcessDwd> processState = new MapStateDescriptor<>("process_state", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = processDwdStream.broadcast(processState);
        //processDwdStream.print();
        //5、连接主流和广播流 对主流数据判断是否要保留
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = jsonObjStream.connect(broadcastStream).process(
                new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    //预加载
                    HashMap<String, TableProcessDwd> hashMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection mysqlConnection = JDBCUtil.getMysqlConnection();
                        List<TableProcessDwd> tableProcessDwds = JDBCUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dwd", TableProcessDwd.class, true);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                            hashMap.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        //将配置表存放到广播状态中
                        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(processState);
                        String op = tableProcessDwd.getOp();
                        String key = tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType();
                        if ("d".equals(op)) {
                            broadcastState.remove(key);
                            hashMap.remove(key);
                        } else {
                            broadcastState.put(key, tableProcessDwd);
                        }
                    }


                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        //调用广播状态判断当前数据是否需要保留
                        String table = jsonObject.getString("table");
                        String type = jsonObject.getString("type");
                        String key = table + ":" + type;

                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(processState);
                        TableProcessDwd processDwd = broadcastState.get(key);
                        //二次判断是否是先到的数据
                        if (processDwd == null) {
                            processDwd = hashMap.get(key);
                        }

                        if (processDwd != null) {
                            collector.collect(Tuple2.of(jsonObject, processDwd));
                        }
                    }
                }).setParallelism(1);

        //processStream.print();
        //筛选最后需要写出的字段你


        SingleOutputStreamOperator<JSONObject> dataStream = filterColumns(processStream);
        //当多个表格的数据写出到同一个主题
        //dataStream.sinkTo(FlinkSinkUtil.getKafkaSink("dwd_base_db"));
        //动态将表写入不同的主题
        dataStream.sinkTo(FlinkSinkUtil.getKafkaSinkWithTopicName());

    }

    public static SingleOutputStreamOperator<JSONObject> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream) {
        return processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDwd processDwd = value.f1;
                JSONObject data = jsonObj.getJSONObject("data");
                List<String> cols = Arrays.asList(processDwd.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !cols.contains(key));
                data.put("sink_table", processDwd.getSinkTable());
                return data;
            }
        });
    }

    public static SingleOutputStreamOperator<JSONObject> flatMapToJsonObj(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("清洗掉脏数据" + value);
                }
            }
        });
    }
}
