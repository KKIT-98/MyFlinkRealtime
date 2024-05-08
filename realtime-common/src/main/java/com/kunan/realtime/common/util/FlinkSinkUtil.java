package com.kunan.realtime.common.util;

import com.kunan.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topicName) {
       return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)  //kafka地址
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)  //写到Kafka的目标主题
                        .setValueSerializationSchema(new SimpleStringSchema()) //序列化器 new SimpleStringSchema() 遇到null会报错 所以不能向kafka发送空值 但这里前置已经对流进行处理不会发送空值 可以使用
                        .build())
               .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) //设置发送类型为精准一次性
               .setTransactionalIdPrefix("kunan-" + topicName + System.currentTimeMillis()) //设置事务ID前缀
               .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "") //设置事务的超时时间 15min
               .build();
    }
}
