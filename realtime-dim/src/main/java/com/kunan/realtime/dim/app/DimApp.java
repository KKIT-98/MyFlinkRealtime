package com.kunan.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.kunan.realtime.common.base.BaseAPP;
import com.kunan.realtime.common.bean.TableProcessDim;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.FlinkSourceUtil;
import com.kunan.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class DimApp extends BaseAPP {
    public static void main(String[] args) {

        new DimApp().start(10001,4,"dim_app", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务逻辑 对数据进行处理
        //1.对ods读取的原始数据进行清洗
        //1.1 第一种方式  分两步、先清洗 再转换
//================================================================================================================================================
        /*stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                boolean flag = false;
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject date = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) && //筛选采集到gmall数据库的数据
                    !"bootstrap-start".equals(type) &&  !"bootstrap-complete".equals(type) //采集的数据type不含bootstrap-start 和 bootstrap-complete （maxwell全量同步标志）
                            && date != null && date.size() != 0)
                        {
                        flag = true;
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }

                return flag;
            }
        }).map(JSONObject::parseObject); //Lambda表达式
                *//*.map(new MapFunction<String, JSONObject>() {//转换
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });*/
//================================================================================================================================================
        //1.2 第一种方式flatmap直接实现  可以使用idea ctrl + alt + m 进行封装 先选中 再按快捷键
       SingleOutputStreamOperator<JSONObject> jsonObjectStream = etl(stream);
      //  System.out.println("===============================对ods读取的原始数据进行清洗==================================");
       // jsonObjectStream.print();

        //2.使用FlinkCDC读取监控配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        DataStreamSource<String> MysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
       //System.out.println("===============================FlinkCDC读取监控配置表数据==================================");
       //MysqlSource.print();
        //3.在Hbase创建维度表

        SingleOutputStreamOperator<TableProcessDim> createHbaseTableStream = createHBaseTable(MysqlSource);

       // System.out.println("===============================在HBASE中创建维度表==================================");
       // createHbaseTableStream.print();

        //4.做成广播流
        //广播状态的key用于判断是否为维度表 value用于补充信息写出到Hbase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = createHbaseTableStream.broadcast(broadcastState);
        //System.out.println("===============================广播流==================================");

        //5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectStream = jsonObjectStream.connect(broadcastStateStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectStream.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
            /**
             *  处理广播数据（配置流数据）
             * @param tableProcessDim
             * @param context
             * @param collector
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(TableProcessDim tableProcessDim
                    , Context context
                    , Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                //读取广播状态
                BroadcastState<String, TableProcessDim> tableProcessState = context.getBroadcastState(broadcastState);
                //将配置表信息作为一个维度表的标记 写到广播状态
                String op = tableProcessDim.getOp();
                if ("d".equals(op)) { //删除
                    tableProcessState.remove(tableProcessDim.getSourceTable());
                    //System.out.println("tableProcessState.remove(tableProcessDim.getSourceTable());");
                } else {
                    tableProcessState.put(tableProcessDim.getSourceTable(), tableProcessDim);
                    //System.out.println(" tableProcessState.put(tableProcessDim.getSourceTable(), tableProcessDim);");
                }

            }

            /**
             * 处理主流数据
             * @param jsonObject
             * @param readOnlyContext
             * @param collector
             * @throws Exception
             */
            @Override
            public void processElement(JSONObject jsonObject
                    , ReadOnlyContext readOnlyContext
                    , Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                //读取广播状态
                ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = readOnlyContext.getBroadcastState(broadcastState);
                //查询广播状态 判断当前数据对应的表格是否存在状态中
                String tableName = jsonObject.getString("table"); //获取表名
                //System.out.println("控制台输出  获取表名" + tableName);
                TableProcessDim tableProcessDim = tableProcessState.get(tableName);
                if (tableProcessDim != null) {
                    //状态不为空 说明当前一行数据是维度表数据 收集数据
                    //System.out.println("控制台输出=> " + tableProcessDim);
                    collector.collect(Tuple2.of(jsonObject, tableProcessDim));
                }
            }
        });
        //System.out.println("===============================连接主流和广播流==================================");
        dimStream.print();
        //6.筛选出需要写出的字段


        //7.写出到Hbase

    }

    public SingleOutputStreamOperator<TableProcessDim> createHBaseTable(DataStreamSource<String> mysqlSource) {
        return mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取连接
                connection = HBaseUtil.getHbaseConnection();
            }

            @Override
            public void close() throws Exception {
                //关闭连接
                HBaseUtil.CloseHbaseConnection(connection);
            }

            @Override
            public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {
                //使用读取的配置表在HBase中创建与之对应的表
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        //当配置表发送D类型的数据 对应Hbase需要删除一张维度表
                        deleteHbaseTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) { //新增的数据
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        CreateHbaseTable(dim);
                    } else {//修改
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteHbaseTable(dim);
                        CreateHbaseTable(dim);
                    }
                    dim.setOp(op);
                    out.collect(dim);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            private void CreateHbaseTable(TableProcessDim dim) {
                String sinkFamily = dim.getSinkFamily();
                String[] split = sinkFamily.split(",");
                try {
                    HBaseUtil.createHbaseTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), split);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            private void deleteHbaseTable(TableProcessDim dim) {
                try {
                    HBaseUtil.DropHbaseTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public  SingleOutputStreamOperator<JSONObject>  etl(SingleOutputStreamOperator<String> stream) {
        // Lambda表达式方式
        return stream.flatMap((FlatMapFunction<String, JSONObject>) (value, out) -> {
            try {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String database = jsonObject.getString("database");
                String type = jsonObject.getString("type");
                JSONObject date = jsonObject.getJSONObject("data");
                if ("gmall".equals(database) && //筛选采集到gmall数据库的数据
                        !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type) //采集的数据type不含bootstrap-start 和 bootstrap-complete （maxwell全量同步标志）
                        && date != null && date.size() != 0) {
                    out.collect(jsonObject);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).returns(JSONObject.class);
    }

}
