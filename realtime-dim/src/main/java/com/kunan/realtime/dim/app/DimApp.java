package com.kunan.realtime.dim.app;

import com.kunan.realtime.common.base.BaseAPP;
import com.kunan.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp extends BaseAPP {
    public static void main(String[] args) {

        new DimApp().start(10001,4,"dim_app", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //核心业务逻辑 对数据进行处理
        //1.对ods读取的原始数据进行清洗
        //2.使用FlinkCDC读取监控配置表数据
        //3.做成广播流
        //4.在Hbase创建维度表
        //5.连接主流和广播流
        //6.筛选出需要写出的字段
        //7.写出到Hbase
        kafkaSource.print();
    }

}
