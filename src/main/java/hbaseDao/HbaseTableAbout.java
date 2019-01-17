package hbaseDao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/17
 * \* Time: 16:23
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class HbaseTableAbout {
    /**
     * 创建表
     * @throws IOException
     */

    public void createTable(String table_name, Configuration configuration) {

        System.out.println("start create table ......");
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            if (hBaseAdmin.tableExists(table_name)) {// 如果存在要创建的表，那么先删除，再创建


                hBaseAdmin.disableTable(table_name);
                hBaseAdmin.deleteTable(table_name);
                System.out.println(table_name + " is exist,detele....");
            }
            // 表的描述
            HTableDescriptor tableDescriptor = new HTableDescriptor(table_name);
//            System.out.println(tableDescriptor);
            //列族的描述
            //HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf_default);
            //设置开启缓存，将表放到RegionServer的缓存中，保证在读取的时候被cache命中。
            //hColumnDescriptor.setInMemory(true);
            //把列族信息加入到 HTableDescriptor 中
            //hTableDescriptor.addFamily(hColumnDescriptor);
            tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
            tableDescriptor.addFamily(new HColumnDescriptor("cf2"));
            tableDescriptor.addFamily(new HColumnDescriptor("cf3"));
            // 创建表,如果存在则删除，重新创建
            hBaseAdmin.createTable(tableDescriptor);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end create table ......");
    }


    /**
     * drop table
     * @param table_name
     */

    public void dropTable(String table_name,Configuration configuration) {
        System.out.println("start drop table ......");
        try {
            //再加上表是否存在的判断
            HBaseAdmin admin = new HBaseAdmin(configuration);
            admin.disableTable(table_name);
            admin.deleteTable(table_name);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end drop table ......");
    }
}