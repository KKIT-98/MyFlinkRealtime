package com.kunan.realtime.common.base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
* 这是一个基类 封装Flink程序入口通用方法
* 为了不同App做处理，使用了抽象类抽象方法
* 基类提供了start()方法，子类只需要调用该方法即可启动数据处理，该方法有四个形参，如下：
* 1）port
* 测试环境下启动本地WebUI的端口，为了避免本地端口冲突，做出以下规定：
* （1）DIM层维度分流应用使用10001端口
* （2）DWD层应用程序按照在本文档中出现的先后顺序，端口从10011开始，自增1
* （3）DWS层应用程序按照在本文档中出现的先后顺序，端口从10021开始，自增1
* 2）parallelism
* 并行度，本项目统一设置为4。
* 3）ckAndGroupId
* 消费Kafka主题时的消费者组ID和检查点路径的最后一级目录名称，二者取值相同，为Job主程序类名的下划线命名形式。如DimApp的该参数取值为dim_app。
* 4）topic
* 消费的Kafka主题名称。
* */
public abstract class BaseAPP {
    public void start(int port,int parallelism,String ckAndGroupId,String topicName){

        //获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        //1.构建Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        // System.setProperty("HADOOP_USER_NAME","yangkunan"); //设定hadoop访问用户
        //2.添加检查点和状态后端参数
        // env.enableCheckpointing(5000L);
        // env.setStateBackend(new HashMapStateBackend());
        // 1.4 状态后端及检查点相关配置
        // 1.4.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 1.4.2 开启 checkpoint
        env.enableCheckpointing(5000);
        // 1.4.3 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 1.4.4 checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.31.102:8020/gmall2023/stream/" + ckAndGroupId);
        // 1.4.5 checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 1.4.6 checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // 1.4.7 checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 1.4.8 job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);


        //3.读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers("192.168.31.102:9092") //设置主机名
                        .setTopics(topicName)//设置topic
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setGroupId(ckAndGroupId)
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(), WatermarkStrategy.<String>noWatermarks(), "kafka_source");
        //4.对数据源进行处理
        //kafkaSource.print();
        //抽象方法
        handle(env,kafkaSource);
        //5.执行环境
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //抽象方法
    public abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> kafkaSource);
}