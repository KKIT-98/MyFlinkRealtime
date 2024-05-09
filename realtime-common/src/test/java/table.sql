-- 读取kafka
CREATE TABLE topic_db (
`database` STRING,
`table` STRING,
`ts` bigint,
`data` map<STRING,STRING>,
`old` map<STRING,STRING>,
`type` STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'topic_db',
      'properties.bootstrap.servers' = 'hadoop102:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json'
);


SELECT
    *
FROM topic_db
WHERE `database` = 'gmall'
AND `table` = 'comment_info';

-- 过滤出comment_info的对应信息

SELECT
`data`['id'] AS id
,`data`['user_id'] as user_id
,`data`['nick_name'] as nick_name
,`data`['head_img'] as head_img
,`data`['sku_id'] as sku_id
,`data`['spu_id'] as spu_id
,`data`['order_id'] as order_id
,`data`['appraise'] as appraise
,`data`['comment_txt'] as comment_txt
,`data`['create_time'] as create_time
,`data`['operate_time'] as operate_time
FROM topic_db
WHERE `database` = 'gmall'
AND `table` = 'comment_info'
AND `type` = 'insert';


SELECT
a.id
,a.user_id
,a.nick_name
,a.head_img
,a.sku_id
,a.spu_id
,a.order_id
,a.appraise AS appraise_code
,b.dic_name as appraise_name
,a.comment_txt
,a.create_time
,a.operate_time
FROM comment_info a
JOIN base_dic b
ON a.appraise = b.dic_code;
-- 表别名区分大小写

-- register a MySQL table 'users' in Flink SQL
CREATE TABLE base_dic (
dic_code STRING,
dic_name STRING,
parent_code STRING,
PRIMARY KEY (dic_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://hadoop102:3306/gmall',
    'table-name' = 'base_dic',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'username' = 'root',
     'password' = '000000'
);

SELECT * FROM base_dic;

