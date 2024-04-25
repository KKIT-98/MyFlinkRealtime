# Flink实时数仓
**介绍**
> 基于尚硅谷Flink实时数仓4.0
---
**前置条件**
> - 尚硅谷大数据项目之电商数仓V6.0 前68节学完
> - 熟悉大数据相关组件
> - 熟悉Linux
>
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