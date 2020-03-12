package com.liukang.conn.hbase;



import com.liukang.conn.hbase.security.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class HBaseUtil {
    private final static Log LOG = LogFactory.getLog(HBaseUtil.class.getName());

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

    private static Configuration conf = null;
    private static String krb5File = null;
    private static String userName = null;
    private static String userKeytabFile = null;

    static{
            try {
                init();
                login();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey) {
        try {

            HTable table = new HTable(conf, tableName);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }
    /**
     * 根据rowkey 查询数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static Result getOneRecord(String tableName, String rowKey){
        try {
            HTable table = new HTable(conf, tableName);
            Get get = new Get(rowKey.getBytes());
            Result rs = table.get(get);
       /* for (KeyValue kv : rs.raw()) {
            System.out.print(new String(kv.getRow()) + " ");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + " ");
            System.out.print(kv.getTimestamp() + " ");
            System.out.println(new String(kv.getValue()));
        }*/
            return rs;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 全表查询
     *
     * @param tableName
     */
    public static ResultScanner getAllRecord(String tableName) {
        try {
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            /*for (Result r : ss) {
                for (KeyValue kv : r.raw()) {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
                return ss;
            }*/
            return  ss ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static List<Map<String, Object>> getTable(String tableName, List<String> startRowKey) {
        List<Map<String, Object>> listMap =new ArrayList<>();
        try {

            HTable table = new HTable(conf, tableName);
            Scan scan = new Scan();
            List<String> list =new ArrayList<>();


            Object[] star = startRowKey.toArray();
//            log.info("$$$$$$$$$$star$$$$$$$$$:"+star.length);
            //统计RowKey重复个数
            Map<String,String> maps= new HashMap<>();
            for (int i = 0; i < startRowKey.toArray().length ; i++) {
                if(StringUtils.isEmpty(maps.get(star[i].toString()))){
                    maps.put(star[i].toString(),"1");
                }else {
                    maps.put(star[i].toString(),String.valueOf(Long.valueOf(maps.get(star[i]))+1));
                }
            }
//            log.info("%%%%%%%%%%maps%%%%%%%%%:"+maps+"maps.size(): "+maps.size());
            List<Map.Entry<String,String>> lists = new ArrayList<Map.Entry<String,String>>(maps.entrySet());
            Collections.sort(lists,new Comparator<Map.Entry<String,String>>() {
                //降序排序
                public int compare(Map.Entry<String, String> o1,
                                   Map.Entry<String, String> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            //添加排序后前3个RowKey
            int i=1;
            List<String> listTop =new ArrayList<>();
            for(Map.Entry<String,String> mapping:lists){
                listTop.add(mapping.getKey());
                if(i==3) break;
                i++;
            }
            for (String str :listTop){
                Get get =new Get(Bytes.toBytes(str));
                Result result =table.get(get);
                for (Cell cl :result.rawCells()){
                    String value = Bytes.toString(CellUtil.cloneValue(cl));
                    list.add(value);
                }
            }
            LOG.info("getTable---> 查出的维表数据 "+list);
            Object[] startRow = listTop.toArray();
            Object[] end = list.toArray();

            for (int j = 0; j < end.length; j++) {
                Map<String, Object> map = new HashMap<>();
                map.put("service_name",end[j].toString().split(",")[6]);
                map.put("service_type",startRow[j]);
                listMap.add(map);
            }

            return listMap;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return listMap;
    }

    private static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        conf.addResource(new Path(userdir + "core-site.xml"));
        conf.addResource(new Path(userdir + "hdfs-site.xml"));
        conf.addResource(new Path(userdir + "hbase-site.xml"));
        LOG.info("hbase init 执行完毕 执行路径为 "+userdir);

    }
    private static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            userName = "jc_zz_gzpoc";
            userKeytabFile = userdir + "user.keytab";
            krb5File = userdir + "krb5.conf";

            /*
             * if need to connect zk, please provide jaas info about zk. of course,
             * you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath +
             * "jaas.conf"); but the demo can help you more : Note: if this process
             * will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY,
                    ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
            LOG.info("hbase login 执行完毕 执行路径为 "+userdir);
        }
    }

}