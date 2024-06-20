# Flink实时数仓
**介绍**
> 基于尚硅谷Flink实时数仓4.0
---
**前置条件**
> - 尚硅谷大数据项目之电商数仓V6.0 前68节学完
> - 熟悉大数据相关组件
> - 熟悉Linux

## 2024-06-20

【事实表动态分流】
1. dwd剩余事实表都是取业务库中的变更数据按条件过滤写到kafka对应主题
2. 逻辑处理相似，可结合配置表动态分流统一处理

关键点: 配置表、预加载、广播流、动态将表写入不同的主题

```sql
--配置表初始化
DROP TABLE IF EXISTS `table_process_dwd`;
CREATE TABLE `table_process_dwd` (
    `source_table` varchar(200) NOT NULL COMMENT '来源表',
    `source_type` varchar(200) NOT NULL COMMENT '来源操作类型',
    `sink_table` varchar(200) NOT NULL COMMENT '输出表',
    `sink_columns` varchar(2000) COMMENT '输出字段',
    PRIMARY KEY (`sink_table`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `table_process_dwd` */
INSERT INTO `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`) VALUES ('coupon_use', 'insert', 'dwd_tool_coupon_get',  'id,coupon_id,user_id,get_time,coupon_status');
INSERT INTO `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`) VALUES ('coupon_use', 'update', 'dwd_tool_coupon_use', 'id,coupon_id,user_id,order_id,using_time,used_time,coupon_status');
INSERT INTO `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`) VALUES ('favor_info', 'insert', 'dwd_interaction_favor_add',  'id,user_id,sku_id,create_time');
INSERT INTO `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`) VALUES ('user_info', 'insert', 'dwd_user_register',  'id,create_time');

```

动态插入kafka不同主题测试:
```bash
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_interaction_favor_add
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_tool_coupon_get
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_tool_coupon_use
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_user_register



```

## 2024-06-19

1. 从topic_db筛选取消订单数据(操作类型为update、修改了order_status字段、修改后的order_status字段值为1003)
2. 取消订单数据与下单事实表(昨天的结果表)关联
3. 关联后宽表发送到kafka
4. 从topic_db筛选支付成功数据
5. 从dwd_trade_order_detail主题中读取订单事实数据、LookUp字典表，关联三张表形成支付成功宽表，写入Kafka支付成功主题。

要点:
- Interval Join: **间隔关联**，关联的两张表需要**事件时间/处理时间**
- Lookup Join: **查找联接**通常用于使用从外部系统查询的数据来丰富表。联接要求一个表具有处理时间属性，另一个表由查找源连接器支持

【测试】

```bash
# 取消订单事实表
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_trade_order_cancel
# 支付成功事实表
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_trade_order_payment_success 
```

## 2024-06-18
1. 筛选订单详情、筛选订单信息表 订单详情活动关联表、订单详情优惠券关联表
2. 以上四张表使用flinkSQL关联
3. 将关联后的结果表发送到kafka（使用了left join 会产生撤回流必须使用upsert kafka连接器）

```bash
#kafka消费数据最终关联后的数据
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_trade_order_detail 

```


:white_check_mark: flink实时项目组件启动顺序
```bash
# 1.hadoop        
start_or_stop_myhadoop.sh start
# 2.zookeeper     
start-zookeeper.sh start
# 3.kafka         
start-kafka.sh start
# 4.hbase(hbase安装包自带)
start-hbase.sh
# 5.maxwell
start-maxwell.sh start

```




## 2024-05-15

筛选加购事务事实表，并写出到kafka

```text
加购样例数据
{"database":"gmall","table":"cart_info","type":"insert","ts":1654695779,"xid":2818373,"commit":true,"data":{"id":3332,"user_id":"480","sku_id":20,"cart_price":2899.00,"sku_num":1,"img_url":null,"sku_name":"apple","is_checked":null,"create_time":"2024-05-15 00:00:00","operate_time":null,"is_ordered":0,"order_time":null}}

下单样例数据: 不是加购
{"database":"gmall","table":"cart_info","type":"update","ts":1654695958,"xid":2818775,"commit":true,"data":{"id":3332,"user_id":"480","sku_id":20,"cart_price":2899.00,"sku_num":1,"img_url":null,"sku_name":"apple","is_checked":null,"create_time":"2024-05-15 00:00:00","operate_time":"2024-05-15 00:00:00","is_ordered":1,"order_time":"2024-05-15 00:30:00"},"old":{"operate_time":null,"is_ordered":0,"order_time":null}}

未下单 继续加购样例数据算加购数据
{"database":"gmall","table":"cart_info","type":"update","ts":1654696074,"xid":2819037,"commit":true,"data":{"id":3326,"user_id":"918","sku_id":26,"cart_price":129.00,"sku_num":3,"img_url":null,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 ","is_checked":null,"create_time":"2024-04-02 11:17:50","operate_time":null,"is_ordered":0,"order_time":null},"old":{"sku_num":1}}
    "sku_num":1  ==> "sku_num":3   
    
未下单 继续减购样例数据 
{"database":"gmall","table":"cart_info","type":"update","ts":1654696234,"xid":2819397,"commit":true,"data":{"id":3326,"user_id":"918","sku_id":26,"cart_price":129.00,"sku_num":2,"img_url":null,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 ","is_checked":null,"create_time":"2024-04-02 11:17:50","operate_time":null,"is_ordered":0,"order_time":null},"old":{"sku_num":3}}
"sku_num":3  ==> "sku_num":2
```


## 2024-05-14
1. 评论表事实表使用lookup join 关联实现维度退化(实际就是评论表关联评论维度表，取评价类型)
2. 关联后的评论事实表写出kafka ```kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_interaction_comment_info```



**遇到bug:**
```text
问题: 关联实现维度退化后打印评论事实表无结果
排查: hbase维度表gmall:dim_base_dic没有数据 排查发现没有将mysql维度表数据同步到kafka
解决: 使用maxwell将维度表初始化全量同步到kafka topic_db中 同步脚本: mysql_to_kafka_dim_init.sh
```

**报错:**
```text
评论事实表写出到kafka报错:
No operators defined in streaming topology. Cannot execute.

解决: 将构建FlinkSQL入口通用方法中的  env.execute(); 去掉即可

```


## 2024-05-13

1. lookup join使用
2. baseSQLAPP封装实现

```text
为什么使用lookup join，而不用inner join
使用inner join flink底层会将两个流都读进内存，当数据越来越多会导致OOM，可以设置过期时间，但是过期后如果数据再有更新，无法实时处理
当主流数据很大，从流数据很小，可使用lookup join来处理，将从流数据读到内存
```



## 2024-05-09

1.FlinkSQL读取Kafka、MySql数据测试
2.FlinkSQL join两张表操作


## 2024-05-08

将拆分后的日志流输出到Kafka

```shell
#启动kafka消费者消费数据验证是否发送到kafka

# 主流
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_traffic_page

#开始页面流
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_traffic_start

#错误信息流
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_traffic_err

#活动页面流
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_traffic_action

#曝光页面流
kafka-console-consumer.sh --bootstrap-server 192.168.31.102:9092 --topic dwd_traffic_display
```


## 2024-05-07

1. 新旧访客修复代码测试

```text

1> 源数据

{"common":{"ar":"14","uid":"763","os":"Android 13.0","ch":"wandoujia","is_new":"0","md":"vivo IQOO Z6x ","mid":"mid_443","vc":"v2.1.134","ba":"vivo","sid":"912dd068-3bd5-43a7-9e85-21dc17bdc7bd"},"page":{"page_id":"cart","during_time":7930,"last_page_id":"good_detail"},"displays":[{"pos_seq":0,"item":"2","item_type":"sku_id","pos_id":5},{"pos_seq":1,"item":"18","item_type":"sku_id","pos_id":5},{"pos_seq":2,"item":"6","item_type":"sku_id","pos_id":5},{"pos_seq":3,"item":"25","item_type":"sku_id","pos_id":5},{"pos_seq":4,"item":"34","item_type":"sku_id","pos_id":5},{"pos_seq":5,"item":"19","item_type":"sku_id","pos_id":5},{"pos_seq":6,"item":"17","item_type":"sku_id","pos_id":5},{"pos_seq":7,"item":"28","item_type":"sku_id","pos_id":5},{"pos_seq":8,"item":"14","item_type":"sku_id","pos_id":5},{"pos_seq":9,"item":"20","item_type":"sku_id","pos_id":5},{"pos_seq":10,"item":"13","item_type":"sku_id","pos_id":5},{"pos_seq":11,"item":"17","item_type":"sku_id","pos_id":5},{"pos_seq":12,"item":"27","item_type":"sku_id","pos_id":5},{"pos_seq":13,"item":"1","item_type":"sku_id","pos_id":5},{"pos_seq":14,"item":"8","item_type":"sku_id","pos_id":5},{"pos_seq":15,"item":"19","item_type":"sku_id","pos_id":5},{"pos_seq":16,"item":"3","item_type":"sku_id","pos_id":5},{"pos_seq":17,"item":"17","item_type":"sku_id","pos_id":5},{"pos_seq":18,"item":"28","item_type":"sku_id","pos_id":5},{"pos_seq":19,"item":"13","item_type":"sku_id","pos_id":5}],"ts":1712026826799}

2> 改造表示新访客

"mid":"mid_443" --> "mid":"mid_4431"
1712026826799 --> 1712113226000
"is_new":"0"  --> "is_new":"1"

{"common":{"ar":"14","uid":"763","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"vivo IQOO Z6x ","mid":"mid_4431","vc":"v2.1.134","ba":"vivo","sid":"912dd068-3bd5-43a7-9e85-21dc17bdc7bd"},"page":{"page_id":"cart","during_time":7930,"last_page_id":"good_detail"},"displays":[{"pos_seq":0,"item":"2","item_type":"sku_id","pos_id":5},{"pos_seq":1,"item":"18","item_type":"sku_id","pos_id":5},{"pos_seq":2,"item":"6","item_type":"sku_id","pos_id":5},{"pos_seq":3,"item":"25","item_type":"sku_id","pos_id":5},{"pos_seq":4,"item":"34","item_type":"sku_id","pos_id":5},{"pos_seq":5,"item":"19","item_type":"sku_id","pos_id":5},{"pos_seq":6,"item":"17","item_type":"sku_id","pos_id":5},{"pos_seq":7,"item":"28","item_type":"sku_id","pos_id":5},{"pos_seq":8,"item":"14","item_type":"sku_id","pos_id":5},{"pos_seq":9,"item":"20","item_type":"sku_id","pos_id":5},{"pos_seq":10,"item":"13","item_type":"sku_id","pos_id":5},{"pos_seq":11,"item":"17","item_type":"sku_id","pos_id":5},{"pos_seq":12,"item":"27","item_type":"sku_id","pos_id":5},{"pos_seq":13,"item":"1","item_type":"sku_id","pos_id":5},{"pos_seq":14,"item":"8","item_type":"sku_id","pos_id":5},{"pos_seq":15,"item":"19","item_type":"sku_id","pos_id":5},{"pos_seq":16,"item":"3","item_type":"sku_id","pos_id":5},{"pos_seq":17,"item":"17","item_type":"sku_id","pos_id":5},{"pos_seq":18,"item":"28","item_type":"sku_id","pos_id":5},{"pos_seq":19,"item":"13","item_type":"sku_id","pos_id":5}],"ts":1712113226000}


3> 把时间改到第二天 模拟程序卸载重装，is_new = 1 伪装成新用户
1712113226000 --> 1712199626000  

{"common":{"ar":"14","uid":"763","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"vivo IQOO Z6x ","mid":"mid_4431","vc":"v2.1.134","ba":"vivo","sid":"912dd068-3bd5-43a7-9e85-21dc17bdc7bd"},"page":{"page_id":"cart","during_time":7930,"last_page_id":"good_detail"},"displays":[{"pos_seq":0,"item":"2","item_type":"sku_id","pos_id":5},{"pos_seq":1,"item":"18","item_type":"sku_id","pos_id":5},{"pos_seq":2,"item":"6","item_type":"sku_id","pos_id":5},{"pos_seq":3,"item":"25","item_type":"sku_id","pos_id":5},{"pos_seq":4,"item":"34","item_type":"sku_id","pos_id":5},{"pos_seq":5,"item":"19","item_type":"sku_id","pos_id":5},{"pos_seq":6,"item":"17","item_type":"sku_id","pos_id":5},{"pos_seq":7,"item":"28","item_type":"sku_id","pos_id":5},{"pos_seq":8,"item":"14","item_type":"sku_id","pos_id":5},{"pos_seq":9,"item":"20","item_type":"sku_id","pos_id":5},{"pos_seq":10,"item":"13","item_type":"sku_id","pos_id":5},{"pos_seq":11,"item":"17","item_type":"sku_id","pos_id":5},{"pos_seq":12,"item":"27","item_type":"sku_id","pos_id":5},{"pos_seq":13,"item":"1","item_type":"sku_id","pos_id":5},{"pos_seq":14,"item":"8","item_type":"sku_id","pos_id":5},{"pos_seq":15,"item":"19","item_type":"sku_id","pos_id":5},{"pos_seq":16,"item":"3","item_type":"sku_id","pos_id":5},{"pos_seq":17,"item":"17","item_type":"sku_id","pos_id":5},{"pos_seq":18,"item":"28","item_type":"sku_id","pos_id":5},{"pos_seq":19,"item":"13","item_type":"sku_id","pos_id":5}],"ts":1712199626000}

使用kafka发送，可以看到控制台打印的结果 is_new = 0，表示修复成功

kafka-console-producer.sh --bootstrap-server hadoop102:9092 --topic topic_log

```

2.使用侧输出流对日志进行拆分实现+功能测试

运行测试，可在控制台查输出的5条流数据

```text

page=> :3> {"common":{"ar":"14","uid":"763","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"vivo IQOO Z6x ","mid":"mid_4431","vc":"v2.1.134","ba":"vivo","sid":"912dd068-3bd5-43a7-9e85-21dc17bdc7bd"},"page":{"page_id":"cart","during_time":7930,"last_page_id":"good_detail"},"ts":1712113226000}
display=> :3> {"pos_seq":1,"item":"18","common":{"ar":"14","uid":"763","os":"Android 13.0","ch":"wandoujia","is_new":"0","md":"vivo IQOO Z6x ","mid":"mid_4431","vc":"v2.1.134","ba":"vivo","sid":"912dd068-3bd5-43a7-9e85-21dc17bdc7bd"},"item_type":"sku_id","pos_id":5,"page":{"page_id":"cart","during_time":7930,"last_page_id":"good_detail"},"ts":1712199626000}
action=> :4> {"item":"3","common":{"ar":"20","uid":"24","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"Redmi k50","mid":"mid_289","vc":"v2.1.134","ba":"Redmi","sid":"d448c905-c20a-40c6-80a8-50f562c8002b"},"action_id":"get_coupon","item_type":"coupon_id","page":{"page_id":"home","refer_id":"1","during_time":14363},"ts":1712025588726}
start=> :4> {"common":{"ar":"22","uid":"716","os":"Android 13.0","ch":"xiaomi","is_new":"1","md":"xiaomi 13","mid":"mid_462","vc":"v2.1.132","ba":"xiaomi","sid":"d2578ffe-0b60-4a0b-b289-da4a3f3e8a45"},"start":{"entry":"install","open_ad_skip_ms":16648,"open_ad_ms":6801,"loading_time":3000,"open_ad_id":8},"ts":1712028525483}
error=> :1> {"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.AppError.main(AppError.java:xxxxxx)","error_code":1795}

```


## 2024-05-06

新旧访客修复代码实现+etl数据测试用例

```text

--测试数据 etl

{"common":{"ar":"11","uid":"137","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"xiaomi 12 ultra ","mid":"mid_485","vc":"v2.1.134","ba":"xiaomi","sid":"c7896d60-d0ec-4ddf-a085-f508c8e78019"},"page":{"page_id":"payment","item":"2783","during_time":6295,"item_type":"order_id","last_page_id":"order"},"ts":1712027492970}

--去掉ts mid

{"common":{"ar":"11","uid":"137","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"xiaomi 12 ultra ","vc":"v2.1.134","ba":"xiaomi","sid":"c7896d60-d0ec-4ddf-a085-f508c8e78019"},"page":{"page_id":"payment","item":"2783","during_time":6295,"item_type":"order_id","last_page_id":"order"}}

--去掉mid
{"common":{"ar":"11","uid":"137","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"xiaomi 12 ultra ","vc":"v2.1.134","ba":"xiaomi","sid":"c7896d60-d0ec-4ddf-a085-f508c8e78019"},"page":{"page_id":"payment","item":"2783","during_time":6295,"item_type":"order_id","last_page_id":"order"},"ts":1712027492970}


--kafka生产发送

kafka-console-producer.sh --bootstrap-server hadoop102:9092 --topic topic_log

查看报错及是否过滤拦截
```

## 2024-05-05

dwd层日志拆分数据清洗etl实现

## 2024-05-02
1.使用JDBC实现配置表预加载（防止主流数据过快导致数据丢失）

2.Hbase写入数据、删除数据工具类实现

3.将dim层数据写入到hbase实现

## 2024-05-01
解决维度表数据新增、删除无法实时监控的bug

1.首先确定在Hbase创建配置维度表是正常的；

2.再排查flinkcdc，也ok，能正常读取mysql中配置的表名；

3.检查读取ods层数据是否正常，打印数据流发现没有数据流进来；

4.从测试直接读取kafka topic_db主题数据，发现能正常读取并打印，更新删除也正常；

5.主程序中读取ods层数据唯一不同是做了数据清洗处理，查看清洗逻辑

  仔细检查后发现在读取数据后将Maxwell监控到的 json格式数据中的key  ```data``` 写成了 ```date``` **导致数据全部被过滤**无法输出到下游
  修改后恢复正常

【回顾该项目数据流转】

首先业务数据在MySQL -> 使用Maxwell实时监控业务数据库数据(开启对应业务数据库的binlog) --> 将监控到变动的数据发送到KafkaTopic -->  flink读取kafka topic数据进行实时处理

## 2024-04-30
动态拆分维度表实现

遇到bug:
注册维度表后维度表的新增、删除、Hbase能同步实时创建表、删除表

但是**维度表数据**的新增、删除 无法体现、教程P33章是正常的


## 2024-04-29
使用配置流信息创建HBase表
```text
启动运行时遇到报错：can not resolve hadoop102....
很明显是域名没解析到，但项目中用到域名已全部替换成了具体IP地址，再三确认没有用到域名，还是报错
最终只能修改本机hosts文件将域名解析配置好，再次运行，成功
```


## 2024-04-28
Hbase创建关闭连接、创建、删除表格API工具类实现

## 2024-4-27
1.对ODS数据进行清洗转换及代码封装 Ctrl+alt+m
2.使用FlinkCDC读取监控配置表数据及封装MySQLSource工具类

## 2024-04-25
1.封装常量类

2.封装kafka连接工具类
    重构SimpleStringSchema()方法 防止写入null值报错4

3.FlinkCDC(监测并捕获数据库的变动)存储配置表实时更新
    创建新库+配置表 开启配置表binlog

```text
报错:  
The MySQL server has a timezone offset (14400 seconds behind UTC) which does not match the configured timezone Asia/Shanghai
处理:修改mysql 配置文件my.conf
添加:
default-time-zone = '+8:00'
重启mysql服务: sudo systemctl restart mysqld
```

## 2024-04-24
基类创建及测试
