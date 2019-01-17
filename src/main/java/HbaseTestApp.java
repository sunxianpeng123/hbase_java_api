import hbaseDao.HbaseCURD;
import hbaseDao.HbaseSelecter;
import hbaseDao.HbaseTableAbout;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/17
 * \* Time: 15:56
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class HbaseTestApp {
    private static final String table_name = "test_api_table";
    private static final String cf_default = "cf1";

    HBaseAdmin admin;

    // 构建一个对象来映射 HBase 的一张表
    // 注意这个对象不是线程安全的
    HTable hTable;

    // 配置信息
    public static Configuration configuration;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");
        configuration.set("hbase.master", "master:600000");
    }

    /**
     * hbase java api
     * @param args
     */
    public static void main(String[] args) {
        String rowKey="112233bbbcccc";
        /**
         * create table
         */
//        HbaseTableAbout hbaseTableAbout =new HbaseTableAbout();
//        hbaseTableAbout.createTable(table_name,configuration);
//         ////drop table
////        hbaseTableAbout.dropTable(table_name,configuration);
//        /**
//         * CURD
//         */
//        HbaseCURD hbaseCURD =new HbaseCURD();
//        //insert
//        hbaseCURD.insertDB(table_name,rowKey,configuration);
////        delete
//        hbaseCURD.deleteRow(table_name,rowKey,configuration);
//        hbaseCURD.insertDB(table_name,rowKey,configuration);
//        //review
//        System.out.println("queryAll===========================");
//        hbaseCURD.queryAll(table_name,configuration);
//        System.out.println("queryByCondition1===========================");
//        hbaseCURD.queryByCondition1(table_name,rowKey,configuration);
//        System.out.println("queryByCondition2===========================");
//        hbaseCURD.queryByCondition2(table_name,configuration);
//        System.out.println("queryByCondition3===========================");
//        hbaseCURD.queryByCondition3(table_name,configuration);
        /**
         * selecter
         */
        HbaseSelecter hbaseSelecter=new HbaseSelecter();
//        hbaseSelecter.selectRowKey(table_name,rowKey,configuration);
        String family ="cf1";
        String column ="tb1";
        System.out.println("------------------------行键  查询----------------------------------");
//        hbaseSelecter.selectRowKeyFamily(table_name,rowKey,family,configuration);
        System.out.println("------------------------行键+列簇 查询----------------------------------");
//        hbaseSelecter.selectRowKeyFamily(table_name,rowKey,family,configuration);
        System.out.println("------------------------行键+列簇+列名 查询----------------------------------");
//        hbaseSelecter.selectRowKeyFamilyColumn(table_name,rowKey,family,column,configuration);
        System.out.println("------------------------条件 查询----------------------------------");
        List<String> arr = new ArrayList<String>();
//        arr.add("cf1,tb2,aaa");//下标0为列簇，1为列名，3为条件
        arr.add("cf2,tb2,bbb");
//        hbaseSelecter.selectFilter(table_name,arr,configuration);

    }







}