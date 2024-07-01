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

--筛选订单详情表数据SQL
SELECT
`data`['id'] id
,`data`['order_id'] order_id
,`data`['sku_id'] sku_id
,`data`['sku_name'] sku_name
,`data`['order_price'] order_price
,`data`['sku_num'] sku_num
,`data`['create_time'] create_time
,`data`['split_total_amount'] split_total_amount
,`data`['split_activity_amount'] split_activity_amount
,`data`['split_coupon_amount'] split_coupon_amount
,ts
FROM topic_db
WHERE  `database` = 'gmall'
AND `table` = 'order_detail'
AND `type` = 'insert'

--筛选订单信息表
SELECT
`data`['id'] id
,`data`['province_id'] province_id
,`data`['user_id'] user_id
,ts
FROM topic_db
WHERE  `database` = 'gmall'
AND `table` = 'order_info'
AND `type` = 'insert'



--筛选订单详情活动关联表

SELECT
`data`['order_detail_id'] order_detail_id
,`data`['activity_id'] activity_id
,`data`['activity_rule_id'] activity_rule_id
FROM topic_db
WHERE  `database` = 'gmall'
AND `table` = 'order_detail_activity'
AND `type` = 'insert'


--筛选订单详情优惠券关联表

SELECT
`data`['order_detail_id'] order_detail_id
,`data`['coupon_id'] coupon_id
FROM topic_db
WHERE  `database` = 'gmall'
AND `table` = 'order_detail_coupon'
AND `type` = 'insert'


--关联以上张表


SELECT
a.id id
,a.order_id order_id
,a.sku_id sku_id
,b.province_id  province_id
,b.user_id  user_id
,c.activity_id activity_id
,c.activity_rule_id activity_rule_id
,d.coupon_id coupon_id
,a.sku_name sku_name
,a.order_price order_price
,a.sku_num sku_num
,a.create_time create_time
,a.split_total_amount split_total_amount
,a.split_activity_amount split_activity_amount
,a.split_coupon_amount split_coupon_amount
,a.ts
FROM order_detail a
JOIN order_info b
ON a.order_id = b.id
LEFT JOIN order_detail_activity c
ON a.id = c.order_detail_id
LEFT JOIN order_detail_coupon d
on a.id = d.order_detail_id

--创建
create table (
id STRING
,order_id STRING
,sku_id STRING
,province_id STRING
,user_id STRING
,activity_id STRING
,activity_rule_id STRING
,coupon_id STRING
,sku_name STRING
,order_price STRING
,sku_num STRING
,create_time STRING
,split_total_amount STRING
,split_activity_amount STRING
,split_coupon_amount STRING
,ts BIGINT
,PRIMARY KEY (id) NOT ENFORCED
)


--从kafka读取dwd层下单事务事实表数据

Constant.TOPIC_DWD_TRADE_ORDER_DETAIL

create table (
id STRING
,order_id STRING
,sku_id STRING
,province_id STRING
,user_id STRING
,activity_id STRING
,activity_rule_id STRING
,coupon_id STRING
,sku_name STRING
,order_price STRING
,sku_num STRING
,create_time STRING
,split_total_amount STRING
,split_activity_amount STRING
,split_coupon_amount STRING
,ts BIGINT
)



--从 topic_db 筛选出订单取消数据

SELECT
`data`['id'] id
,`data`['operate_time'] operate_time
,ts
FROM topic_db
WHERE `database` = 'gmall'
AND `table` = 'order_info'
AND `type` = 'update'
AND `old`['order_status'] = '1001'
AND `data`['order_status'] = '1003'

--订单取消表和下单表进行 join

SELECT
 a.id as id
,a.order_id as order_id
,a.sku_id as sku_id
,a.province_id as province_id
,a.user_id as user_id
,a.activity_id as activity_id
,a.activity_rule_id as activity_rule_id
,a.coupon_id as coupon_id
,date_format(b.operate_time, 'yyyy-MM-dd') as order_cancel_date_id
,b.operate_time as operate_time
,a.sku_name as sku_name
,a.order_price as order_price
,a.sku_num as sku_num
,a.create_time as create_time
,a.split_total_amount as split_total_amount
,a.split_activity_amount as split_activity_amount
,a.split_coupon_amount as split_coupon_amount
,b.ts as ts
FROM Constant.TOPIC_DWD_TRADE_ORDER_DETAIL a
JOIN order_cancel b
ON a.order_id = b.id


--将关联后的宽表写出到kafka



create table dwd_trade_order_cancel_detail(
 id STRING
,order_id STRING
,sku_id STRING
,province_id STRING
,user_id STRING
,activity_id STRING
,activity_rule_id STRING
,coupon_id STRING
,order_cancel_date_id STRING
,operate_time STRING
,sku_name STRING
,order_price STRING
,sku_num STRING
,create_time STRING
,split_total_amount STRING
,split_activity_amount STRING
,split_coupon_amount STRING
,ts BIGINT
)

--从topic_db筛选支付成功的数据


SELECT
`data`['id'] AS id
,`data`['order_id'] AS order_id
,`data`['user_id'] AS user_id
,`data`['payment_type'] AS payment_type
,`data`['total_amount'] AS total_amount --订单总金额
,`data`['callback_time'] AS callback_time
,ts
FROM topic_db
WHERE  `database` = 'gmall'
AND `table` = 'payment_info'
AND `type` = 'update'
AND `old`['payment_status'] IS NOT NULL
AND `data`['payment_status'] = '1602'

--使用interval join完成支付成功流和订单流数据关联


SELECT
b.id AS id
,a.order_id AS order_id
,a.user_id AS user_id
,a.payment_type AS payment_type
,a.callback_time AS payment_time
,b.sku_id AS sku_id
,b.province_id AS province_id
,b.activity_id AS activity_id
,b.activity_rule_id AS activity_rule_id
,b.coupon_id AS coupon_id
,b.sku_name AS sku_name
,b.order_price AS order_price
,b.sku_num AS sku_num
,b.split_total_amount AS split_total_amount
,b.split_activity_amount AS split_activity_amount
,b.split_coupon_amount AS split_coupon_amount
,a.ts AS ts
FROM payment a,order_detail b
where a.order_id = b.order_id
AND a.row_time BETWEEN b.row_time - INTERVAL '15' MINUTE AND b.row_time + INTERVAL '5' SECOND


--使用lookup join完成维度退化

SELECT
 a.id AS id
,a.order_id AS order_id
,a.user_id AS user_id
,a.payment_type AS payment_type_code
,b.info.dic_name AS payment_type_name
,a.payment_time AS payment_time
,a.sku_id AS sku_id
,a.province_id AS province_id
,a.activity_id AS activity_id
,a.activity_rule_id AS activity_rule_id
,a.coupon_id AS coupon_id
,a.sku_name AS sku_name
,a.order_price AS order_price
,a.sku_num AS sku_num
,a.split_total_amount AS split_total_amount
,a.split_activity_amount AS split_activity_amount
,a.split_coupon_amount AS split_coupon_amount
,a.ts AS ts
FROM pay_order a
LEFT JOIN base_dic FOR SYSTEM_TIME AS OF a.proc_time AS b
ON a.payment_type = b.rowkey


--lookup join 示例
--SELECT o.order_id, o.total, c.country, c.zip
--FROM Orders AS o
--  JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
--    ON o.customer_id = c.id;


--创建结果集表

CREATE TABLE (
 id STRING
,order_id STRING
,user_id STRING
,payment_type_code STRING
,payment_type_name STRING
,payment_time STRING
,sku_id STRING
,province_id STRING
,activity_id STRING
,activity_rule_id STRING
,coupon_id STRING
,sku_name STRING
,order_price STRING
,sku_num STRING
,split_total_amount STRING
,split_activity_amount STRING
,split_coupon_amount STRING
,ts BIGINT
)

--------------------------------------------------------------
--【DWS层】

CREATE TABLE page_info(
`common` map<STRING,STRING>
,`page` map<STRING,STRING>
,`ts` bigint
)

SELECT
page['item']  keyword
,`ts`
FROM page_info
WHERE page['last_page_id'] = 'search'
AND page['item_type'] = 'keyword'
AND page['item'] IS NOT NULL