package com.kunan.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.kunan.realtime.common.bean.TableProcessDim;
import com.kunan.realtime.common.util.JDBCUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    public HashMap<String,TableProcessDim> hashMap;
    MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //预加载初始的维度表信息:解决主流数据过快导致数据丢失
        java.sql.Connection mysqlConnection = JDBCUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JDBCUtil.queryList(mysqlConnection, "SELECT x.* FROM gmall2023_config.table_process_dim x", TableProcessDim.class, true);
        hashMap = new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JDBCUtil.closeConnection(mysqlConnection);
    }

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
            //同步删除hashmap加载的配置表信息
            hashMap.remove(tableProcessDim.getSourceTable());
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
        //如果是数据到的太早导致状态为空 添加一层当前属性的初始化方法
        if(tableProcessDim == null) {
            tableProcessDim = hashMap.get(tableName);
        }
        if (tableProcessDim != null) {
            //状态不为空 说明当前一行数据是维度表数据 收集数据
            //System.out.println("控制台输出=> " + tableProcessDim);
            collector.collect(Tuple2.of(jsonObject, tableProcessDim));
        }
    }
}
