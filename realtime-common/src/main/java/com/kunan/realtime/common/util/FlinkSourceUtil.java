package com.kunan.realtime.common.util;
import com.kunan.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class FlinkSourceUtil {
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
}
