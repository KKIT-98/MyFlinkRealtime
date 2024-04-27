package com.kunan.realtime.common.util;
import com.kunan.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class FlinkSourceUtil {
    //KafkaSource工具类
    public static KafkaSource<String> getKafkaSource(String ckAndGroupId,String topicName){
        return KafkaSource.<String>builder()
                        .setBootstrapServers(Constant.KAFKA_BROKERS) //设置kafka连接地址
                        .setTopics(topicName)//设置topic
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setGroupId(ckAndGroupId)
                        .setValueOnlyDeserializer(
                                //SimpleStringSchema 无法反序列化null值数据 会直接报错
                                //后续DWD层会向kafka中发送null，为防止报错，不能直接使用SimpleStringSchema 需重构该方法:
                                //new SimpleStringSchema()
                                new DeserializationSchema<String>() {
                                    @Override
                                    public String deserialize(byte[] message) throws IOException {
                                        if (message != null && message.length != 0){
                                            return new String(message, StandardCharsets.UTF_8);
                                        }
                                        return "";
                                    }

                                    @Override
                                    public boolean isEndOfStream(String nextElement) {
                                        return false;
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                }
                        )
                        .build();

    }
    //MySQLSource工具类
    public static MySqlSource<String> getMySqlSource(String databaseName,String tableName){
        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList(databaseName)
                .tableList(databaseName + "." + tableName) //因为支持从多个库读取，会出现找不到情况，这里使用库名.表名的方式
                .deserializer(new JsonDebeziumDeserializationSchema()) //json格式反序列化
                .startupOptions(StartupOptions.initial())  //初始化读取方式
                .build();
    }
}
