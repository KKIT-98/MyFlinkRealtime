# Flink实时数仓
**介绍**
> 基于尚硅谷Flink实时数仓4.0
---
**前置条件**
> - 尚硅谷大数据项目之电商数仓V6.0 前68节学完
> - 熟悉大数据相关组件
> - 熟悉Linux
>
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
```java
报错:  
The MySQL server has a timezone offset (14400 seconds behind UTC) which does not match the configured timezone Asia/Shanghai

处理:修改mysql 配置文件my.conf
添加:
default-time-zone = '+8:00'
重启mysql服务: sudo systemctl restart mysqld

```

## 2024-04-24
基类创建及测试
