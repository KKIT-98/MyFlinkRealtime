# Flink实时数仓
**介绍**
> 基于尚硅谷Flink实时数仓4.0
---
**前置条件**
> - 尚硅谷大数据项目之电商数仓V6.0 前68节学完
> - 熟悉大数据相关组件
> - 熟悉Linux
>
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