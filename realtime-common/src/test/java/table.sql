-- 读取kafka
CREATE TABLE topic_db (
`database` STRING,
`table` STRING,
`ts` bigint,
`data` map<STRING,STRING>,
`old` map<STRING,STRING>,
`type` STRING,
proc_time  AS PROCTIME()
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
,proc_time
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
JOIN base_dic FOR SYSTEM_TIME AS OF a.proc_time b
ON a.appraise = b.dic_code;



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


---------------------------------------------
--How to use HBase table
--All the column families in HBase table must be declared as ROW type, the field name maps to the column family name, and the nested field names map to the column qualifier names. There is no need to declare all the families and qualifiers in the schema, users can declare what’s used in the query. Except the ROW type fields, the single atomic type field (e.g. STRING, BIGINT) will be recognized as HBase rowkey. The rowkey field can be arbitrary name, but should be quoted using backticks if it is a reserved keyword.
CREATE TABLE base_dic (
rowkey STRING,
info ROW<dic_name STRING>,
PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'gmall:dim_base_dic ',
 'zookeeper.quorum' = '192.168.31.102:2181'
);



--id STRING
--,user_id STRING
--,nick_name STRING
--,sku_id STRING
--,spu_id STRING
--,order_id STRING
--,appraise_code STRING
--,appraise_name STRING
--,comment_txt STRING
--,create_time STRING
--,operate_time STRING

--------------------------------
--筛选加购数据
--------------------------------
--加购样例数据:
--{"database":"gmall","table":"cart_info","type":"insert","ts":1654695779,"xid":2818373,"commit":true,"data":{"id":3332,"user_id":"480","sku_id":20,"cart_price":2899.00,"sku_num":1,"img_url":null,"sku_name":"apple","is_checked":null,"create_time":"2024-05-15 00:00:00","operate_time":null,"is_ordered":0,"order_time":null}}
--下单样例数据: 不是加购
--{"database":"gmall","table":"cart_info","type":"update","ts":1654695958,"xid":2818775,"commit":true,"data":{"id":3332,"user_id":"480","sku_id":20,"cart_price":2899.00,"sku_num":1,"img_url":null,"sku_name":"apple","is_checked":null,"create_time":"2024-05-15 00:00:00","operate_time":"2024-05-15 00:00:00","is_ordered":1,"order_time":"2024-05-15 00:30:00"},"old":{"operate_time":null,"is_ordered":0,"order_time":null}}
--未下单 继续加购样例数据
--{"database":"gmall","table":"cart_info","type":"update","ts":1654696074,"xid":2819037,"commit":true,"data":{"id":3326,"user_id":"918","sku_id":26,"cart_price":129.00,"sku_num":3,"img_url":null,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 ","is_checked":null,"create_time":"2024-04-02 11:17:50","operate_time":null,"is_ordered":0,"order_time":null},"old":{"sku_num":1}}
--"sku_num":1  ==> "sku_num":3   算加购数据
--未下单 继续减购样例数据
---{"database":"gmall","table":"cart_info","type":"update","ts":1654696234,"xid":2819397,"commit":true,"data":{"id":3326,"user_id":"918","sku_id":26,"cart_price":129.00,"sku_num":2,"img_url":null,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 ","is_checked":null,"create_time":"2024-04-02 11:17:50","operate_time":null,"is_ordered":0,"order_time":null},"old":{"sku_num":3}}
--"sku_num":3  ==> "sku_num":2


SELECT
`data`['id']   AS id
,`data`['user_id'] AS user_id
,`data`['sku_id'] AS sku_id
,`data`['cart_price'] AS cart_price
,IF(`type` = 'insert',`data`['sku_num'],CAST((CAST(`data`['sku_num'] AS BIGINT) - CAST(`old`['sku_num'] AS BIGINT)) AS STRING))  AS sku_num
,`data`['sku_name'] AS sku_name
,`data`['is_checked']  AS is_checked
,`data`['create_time']  AS create_time
,`data`['operate_time']  AS operate_time
,`data`['is_ordered']  AS is_ordered
,`data`['order_time']  AS order_time
,ts
FROM topic_db
WHERE `database` = 'gmall'
AND `table` = 'cart_info'
AND (`type` = 'insert'
         OR
     (`type` = 'update' AND `old`['sku_num'] IS NOT NULL AND CAST(`data`['sku_num'] AS BIGINT) > CAST(`old`['sku_num'] AS BIGINT))
    )
;

--输出到kafka映射

CREATE TABLE dwd_trade_cart_add(
id STRING
,user_id STRING
,sku_id STRING
,cart_price STRING
,sku_num STRING
,sku_name STRING
,is_checked STRING
,create_time STRING
,operate_time STRING
,is_ordered STRING
,order_time STRING
,ts BIGINT
)