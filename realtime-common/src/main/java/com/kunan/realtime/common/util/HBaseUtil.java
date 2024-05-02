package com.kunan.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.kunan.realtime.common.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {
    /**
     * 获取Hbase连接
     * @return 一个hbase的同步连接
     */

    public static Connection getHbaseConnection(){
        //1.使用conf
       // Configuration conf = new Configuration();
       // conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
       // Connection connection = ConnectionFactory.createConnection(conf);

        //2.使用配置文件 把hbase安装目录conf下hbase-site.xml 复制到common模块下的resource文件夹下面
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection();
        }catch (IOException e){
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭Hbase连接
     * @param connection 一个hbase的同步连接
     */
    //
    public static void CloseHbaseConnection(Connection connection)  {
        if (connection != null && !connection.isClosed()){
           try {
               connection.close();
           }catch (IOException e){
               e.printStackTrace();
           }
        }
    }
/**
 * hbase命令行操作<br>
 * 创建命名空间 create_namespace 'gmall' <br>
 * 创建表 create 'gmall.test','info'  命名空间.表名 列簇名 <br>
 * 删除表 先禁用 disable 'gmall.test'  再删除    drop 'gmall.test' <br>
 * **/
    /**
     * 创建HBase表
     * @param connection 一个hbase的同步连接
     * @param namespace 命名空间名称
     * @param table 表名
     * @param families 列簇名 可以是多个
     * @throws IOException 获取admin连接异常
     */
    public static void createHbaseTable(Connection connection,String namespace,String table,String... families) throws IOException {
        if (families.length == 0 || families == null ){
            System.out.println("创建Hbase表至少需要一个列簇!");
            return;
        }

        //1.获取admin
        Admin admin = connection.getAdmin();
        //2.创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));
        for (String family : families){
            //创建列簇描述
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
                    .build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }

        //3.使用admin调用方法创建表格
        try {//不影响主程序运行 捕获异常
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("当前Hbase表已存在，无需重复创建" + "【" + namespace + ":" + table + "】");
        }

        //4.关闭admin
        admin.close();

    }

    /**
     * 删除表格
     * @param connection 一个hbase的同步连接
     * @param namespace 命名空间名称
     * @param table  表名
     * @throws IOException 获取admin连接异常
     */
    public static void DropHbaseTable(Connection connection,String namespace,String table) throws IOException {
        //1.获取admin
        Admin admin = connection.getAdmin();
        //2.调用方法删除表格
        try {
            admin.disableTable(TableName.valueOf(namespace,table));
            admin.deleteTable(TableName.valueOf(namespace,table));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //3.关闭admin
        admin.close();
    }

    /**
     * 写入数据到HBase的方法
     * @param connection 一个同步连接
     * @param namespace 命名空间名称
     * @param tableName 表名
     * @param rowKey 主键
     * @param family 列簇名
     * @param data   列名和列值 jsonObject对象
     * @throws IOException
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        // 1、获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        // 2、创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String column : data.keySet()) {

            String columnValue = data.getString(column);
            if (columnValue != null ){
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(columnValue));
            }

        }
        // 3、调用方法写出数据
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 4、关闭table
        table.close();
    }

    /**
     * 删除hbase一整行数据
     * @param connection  一个同步连接
     * @param namespace 命名空间名称
     * @param tableName 表名
     * @param rowKey 主键
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        // 1、获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        // 2、创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 3、调用方法删除数据
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 4、关闭table
        table.close();
    }
}
