package hbaseDao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
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
/**
 *Hbase基本使用示例
 */
public class HbaseCURD {
    /**
     * 插入数据 && 批量插入
     */
    public void insertDB(String table_name,String rowKey,Configuration configuration)  {
        System.out.println("start insert data ......");
        try {

            // HTable 对象共享配置的好处：共享ZooKeeper的连接；共享公共的资源：客户端需要通过ZooKeeper查找-ROOT-和.META.表，
            // 这个需要网络传输开销，客户端缓存这些公共资源后能够减少后续的网络传输开销，加快查找过程速度
            HTable table = new HTable(configuration,table_name);
            // HTablePool pool = new HTablePool(configuration, 1000);
            //HTable table = (HTable) pool.getTable(table_name);
            // 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
            Put put = new Put(rowKey.getBytes());
            put.add("cf1".getBytes(), "tb1".getBytes(), "aaa".getBytes());// 本行数据的第一列
            put.add("cf1".getBytes(), "tb2".getBytes(), "aaa".getBytes());// 本行数据的第一列
            put.add("cf2".getBytes(), "tb1".getBytes(), "bbb".getBytes());// 本行数据的第三列
            put.add("cf2".getBytes(), "tb2".getBytes(), "bbb".getBytes());// 本行数据的第三列
            put.add("cf3".getBytes(), "tb1".getBytes(), "ccc".getBytes());// 本行数据的第三列
            put.add("cf3".getBytes(), "tb2".getBytes(), "ccc".getBytes());// 本行数据的第三列
            //单条插入
            table.put(put);
            //// 为了节省资源的占用，可以批量插入，比如
//            List<Put> list = new ArrayList<Put>();
//            list.add(put);
//            list.add(put2);
//            table.put(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end insert data ......");

    }

    /**
     *根据rowKey删除一行
     */

    public  void deleteRow(String tablename, String rowkey,Configuration configuration)  {
        try {
            HTable table = new HTable(configuration, tablename);
            List list = new ArrayList();
            Delete d1 = new Delete(rowkey.getBytes());
            list.add(d1);

            table.delete(list);
            System.out.println("删除行成功!");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询
     */
    public  void queryAll(String tableName,Configuration configuration) {
        try {
        HTablePool pool = new HTablePool(configuration, 1000);
        HTable table = new HTable(configuration, tableName);

            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //根据rowkey查询
    public void queryByCondition1(String tableName,String rowKey,Configuration configuration) {
        try {
        HTablePool pool = new HTablePool(configuration, 1000);
        HTable table = new HTable(configuration, tableName);

            Get scan = new Get(rowKey.getBytes());// 根据rowkey查询
            Result r = table.get(scan);
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列：" + new String(keyValue.getFamily())
                        + "====值:" + new String(keyValue.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //
    public  void queryByCondition2(String tableName,Configuration configuration) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);
            HTable table = new HTable(configuration, tableName);
            Filter filter = new SingleColumnValueFilter(Bytes
                    .toBytes("cf1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("aaa")); // 当列column1的值为aaa时进行查询
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //
    public  void queryByCondition3(String tableName,Configuration configuration) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);
            HTable table = new HTable(configuration, tableName);

            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new SingleColumnValueFilter(Bytes
                    .toBytes("cf1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("aaa"));//当列column1的值为aaa时进行查询
            filters.add(filter1);

            Filter filter2 = new SingleColumnValueFilter(Bytes
                    .toBytes("cf2"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("bbb"));//当列column1的值为bbb时进行查询
            filters.add(filter2);

            Filter filter3 = new SingleColumnValueFilter(Bytes
                    .toBytes("cf3"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("ccc"));//当列column1的值为ccc时进行查询
            filters.add(filter3);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}