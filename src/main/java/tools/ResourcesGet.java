package tools;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/17
 * \* Time: 15:59
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class ResourcesGet {

    public  String  getConfig(String key){
        String value ="";
        try{
            Properties properties= new Properties();
            String path=System.getProperty("user.dir")+"/db_hbaseJavaApiStudy.properties";
//            String path="/db_hbaseJavaApiStudy.properties";
            InputStream is= new FileInputStream(path);
            properties.load(is);
            value =properties.getProperty(key);
//            System.out.println("###########DBIP:"+properties.getProperty("dbIp"));
//            conn = DriverManager.getConnection("jdbc:mysql://"+properties.getProperty("dbIp")+":"+properties.getProperty("dbPort")+"/"
//                            +properties.getProperty("dbName")+"?autoReconnect=true&rewriteBatchedStatements=true&useUnicode=true&characterEncoding=utf8&useSSL=false",
//                    properties.getProperty("dbUser"),  properties.getProperty("dbPassword"));
            is.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return value;
    }



}