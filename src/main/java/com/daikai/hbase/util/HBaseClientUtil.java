package com.daikai.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @autor kevin.dai
 * @Date 2018/1/10
 */
public class HBaseClientUtil {

    private  static  Configuration configuration;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("hbase.zookeeper.quorum","dataforce01,dataforce02,dataforce03");
    }


    /**
     * 查询
     * @Date: 10:10 2018/1/10
     */
    public static Result getResult(String tableName, String rowkey) {
        try {
            HTable table = new HTable(configuration, tableName);
            Get get = new Get(rowkey.getBytes());
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    // 创建表
    public static boolean create(String tableName, String columnFamily) {
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(configuration);
            if (admin.tableExists(tableName)) {
                System.out.println(tableName + " exists!");
                return false;
            } else {
                // 逗号分隔，可以有多个columnFamily
                String[] cfArr = columnFamily.split(",");
                HColumnDescriptor[] hcDes = new HColumnDescriptor[cfArr.length];
                for (int i = 0; i < cfArr.length; i++) {
                    hcDes[i] = new HColumnDescriptor(cfArr[i]);
                }
                HTableDescriptor tblDes = new HTableDescriptor(TableName.valueOf(tableName));
                for (HColumnDescriptor hc : hcDes) {
                    tblDes.addFamily(hc);
                }
                admin.createTable(tblDes);
                System.out.println(tableName + " create successfully！");
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    //插入数据
    public static boolean put(String tableName, String rowkey, String columnFamily, String qualifier, String value) {
        try {
            HTable table = new HTable(configuration, tableName);
            Put put = new Put(rowkey.getBytes());
            put.add(columnFamily.getBytes(), qualifier.getBytes(), value.getBytes());
            table.put(put);
            System.out.println("put successfully！ " + rowkey + "," + columnFamily + "," + qualifier + "," + value);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }



    //指定qualifier查询数据
    public static byte[] get(String tableName, String rowkey, String qualifier) {
        System.out.println("get result. table=" + tableName + " rowkey=" + rowkey + " qualifier=" + qualifier);
        Result result = getResult(tableName, rowkey);
        if (result != null && result.listCells() != null) {
            for (Cell cell : result.listCells()) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (key.equals(qualifier)) {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(key + " => " + value);
                    return CellUtil.cloneValue(cell);
                }
            }
        }
        return null;
    }


    /**
     * 结果输出
     * @Date: 10:10 2018/1/10
     */
    private static Map<String, Object> result2Map(Result result) {
        Map<String, Object> ret = new HashMap<String, Object>();
        if (result != null && result.listCells() != null) {
            for (Cell cell : result.listCells()) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(key + " => " + value);
                ret.put(key, value);
            }
        }
        return ret;
    }


    public static void main(String[] args) {
        Result result =  HBaseClientUtil.getResult("user","10001");
        HBaseClientUtil.result2Map(result);
    }


}
