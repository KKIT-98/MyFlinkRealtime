package com.kunan.realtime.dim.app;

import com.kunan.realtime.common.base.BaseAPP;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp extends BaseAPP {
    public static void main(String[] args) {

        new DimApp().start(10001,4,"dim_app","topic_db");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        //核心业务逻辑 对数据进行处理
        kafkaSource.print();
    }
}
