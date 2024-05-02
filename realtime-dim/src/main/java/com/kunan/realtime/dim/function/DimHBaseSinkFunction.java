package com.kunan.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.kunan.realtime.common.bean.TableProcessDim;
import com.kunan.realtime.common.constant.Constant;
import com.kunan.realtime.common.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class DimHBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取连接
        connection = HBaseUtil.getHbaseConnection();
    }
    @Override
    public void close() throws Exception {
        HBaseUtil.CloseHbaseConnection(connection);
    }
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObject = value.f0;
        TableProcessDim dim = value.f1;
        //insert update delete bootstrap-insert
        String type = jsonObject.getString("type");
        JSONObject data = jsonObject.getJSONObject("data");
        if("delete".equals(type)){
            //删除对应的维度表数据
            delete(data,dim);

        }else {
            //覆盖写入维度表数据
            put(data,dim);
        }
    }

    private void put(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        String sinkFamily = dim.getSinkFamily();
        try {
            HBaseUtil.putCells(connection, Constant.HBASE_NAMESPACE,sinkTable,sinkRowKeyValue,sinkFamily,data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void delete(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        try {
            HBaseUtil.deleteCells(connection,Constant.HBASE_NAMESPACE,sinkTable,sinkRowKeyValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
