package hbaseDao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/17
 * \* Time: 20:57
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */

/**
 * Hbase数据获取示例
 */
public class HbaseSelecter {
    public  void selectRowKey(String tablename, String rowKey, Configuration configuration){
        try {
            HTable table = new HTable(configuration, tablename);
            Get g = new Get(rowKey.getBytes());
            Result rs = table.get(g);

            for (KeyValue kv : rs.raw()) {
                System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
                System.out.println("Column Family: " + new String(kv.getFamily()));
                System.out.println("Column       :" + new String(kv.getQualifier()));
                System.out.println("value        : " + new String(kv.getValue()));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public void selectRowKeyFamily(String tablename, String rowKey, String family, Configuration configuration)
    {
        try {
            HTable table = new HTable(configuration, tablename);
            Get g = new Get(rowKey.getBytes());
            g.addFamily(Bytes.toBytes(family));
            Result rs = table.get(g);
            for (KeyValue kv : rs.raw())
            {
                System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
                System.out.println("Column Family: " + new String(kv.getFamily()));
                System.out.println("Column       :" + new String(kv.getQualifier()));
                System.out.println("value        : " + new String(kv.getValue()));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }




    public  void selectRowKeyFamilyColumn(String tablename, String rowKey, String family, String column ,Configuration configuration){
        try {
            HTable table = new HTable(configuration, tablename);
            Get g = new Get(rowKey.getBytes());
            g.addColumn(family.getBytes(), column.getBytes());

            Result rs = table.get(g);

            for (KeyValue kv : rs.raw())
            {
                System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
                System.out.println("Column Family: " + new String(kv.getFamily()));
                System.out.println("Column       :" + new String(kv.getQualifier()));
                System.out.println("value        : " + new String(kv.getValue()));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public  void selectFilter(String tablename, List<String> arr,Configuration configuration){
        try{
            HTable table = new HTable(configuration, tablename);
            Scan scan = new Scan();// 实例化一个遍历器
            FilterList filterList = new FilterList(); // 过滤器List

            for (String v : arr)
            { // 下标0为列簇，1为列名，3为条件
                String[] wheres = v.split(",");

                filterList.addFilter(new SingleColumnValueFilter(// 过滤器
                        wheres[0].getBytes(), wheres[1].getBytes(),

                        CompareFilter.CompareOp.EQUAL,// 各个条件之间是" and "的关系
                        wheres[2].getBytes()));
            }
            scan.setFilter(filterList);
            ResultScanner ResultScannerFilterList = table.getScanner(scan);
            for (Result rs = ResultScannerFilterList.next(); rs != null; rs = ResultScannerFilterList.next())
            {
                for (KeyValue kv : rs.list())
                {
                    System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
                    System.out.println("Column Family: " + new String(kv.getFamily()));
                    System.out.println("Column       :" + new String(kv.getQualifier()));
                    System.out.println("value        : " + new String(kv.getValue()));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}