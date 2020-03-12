package com.liukang.conn.hbase.examples;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * HBase Development Instruction Sample Code The sample code uses user
 * information as source data,it introduces how to implement businesss process
 * development using HBase API
 */
@Slf4j
public class HBaseSample {
  //private final static log log = logFactory.getlog(HBaseSample.class.getName());

  private TableName tableName = null;
  private Configuration conf = null;
  private Connection conn = null;

  public HBaseSample(Configuration conf) throws IOException {
    this.conf = conf;
    this.tableName = TableName.valueOf("hbase_sample_table");
    this.conn = ConnectionFactory.createConnection(conf);
  }

  public void test() throws Exception {
    try {
      testCreateTable();
      //testMultiSplit();
      testPut();
    //  createIndex();
      testScanDataByIndex();
      testModifyTable();
      testGet();
      testScanData();
      testSingleColumnValueFilter();
      testFilterList();
      testDelete();
   //   dropIndex();
      dropTable();
    //  testCreateMOBTable();
      testMOBDataInsertion();
      testMOBDataRead();
      dropTable();
    } catch (Exception e) {
      throw e;
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (Exception e1) {
          log.error("Failed to close the connection ", e1);
        }
      }
    }
  }

  /**
   * Create user info table
   */
  public void testCreateTable() {
    log.info("Entering testCreateTable.");

    // Specify the table descriptor.
    HTableDescriptor htd = new HTableDescriptor(tableName);

    // Set the column family name to info.
    HColumnDescriptor hcd = new HColumnDescriptor("info");

    // Set data encoding methods. HBase provides DIFF,FAST_DIFF,PREFIX
    // and PREFIX_TREE
    hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

    // Set compression methods, HBase provides two default compression
    // methods:GZ and SNAPPY
    // GZ has the highest compression rate,but low compression and
    // decompression effeciency,fit for cold data
    // SNAPPY has low compression rate, but high compression and
    // decompression effeciency,fit for hot data.
    // it is advised to use SANPPY
    hcd.setCompressionType(Compression.Algorithm.SNAPPY);

    htd.addFamily(hcd);

    Admin admin = null;
    try {
      // Instantiate an Admin object.
      admin = conn.getAdmin();
      if (!admin.tableExists(tableName)) {
        log.info("Creating table...");
        admin.createTable(htd);
        log.info(""+admin.getClusterStatus());
        log.info(""+admin.listNamespaceDescriptors());
        log.info("Table created successfully.");
      } else {
        log.warn("table already exists");
      }
    } catch (IOException e) {
      log.error("Create table failed.", e);
    } finally {
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          log.error("Failed to close admin ", e);
        }
      }
    }
    log.info("Exiting testCreateTable.");
  }

/*  public void testMultiSplit() {
    log.info("Entering testMultiSplit.");

    Table table = null;
    Admin admin = null;
    try {
      admin = conn.getAdmin();

      // initilize a HTable object
      table = conn.getTable(tableName);
      Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>();
      List<HRegionLocation> regionList = conn.getRegionLocator(tableName).getAllRegionLocations();
      for (HRegionLocation hrl : regionList) {
        regionSet.add(hrl.getRegionInfo());
      }
      byte[][] sk = new byte[4][];
      sk[0] = "A".getBytes();
      sk[1] = "D".getBytes();
      sk[2] = "F".getBytes();
      sk[3] = "H".getBytes();
      for (HRegionInfo regionInfo : regionSet) {
        ((HBaseAdmin) admin).multiSplit(regionInfo.getRegionName(), sk);
      }
      log.info("MultiSplit successfully.");
    } catch (Exception e) {
      log.error("MultiSplit failed ", e);
    } finally {
      if (table != null) {
        try {
          // Close table object
          table.close();
        } catch (IOException e) {
          log.error("Close table failed ", e);
        }
      }
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          log.error("Close admin failed ", e);
        }
      }
    }
    log.info("Exiting testMultiSplit.");
  }*/

  /**
   * Insert data
   */
  public void testPut() {
    log.info("Entering testPut.");

    // Specify the column family name.
    byte[] familyName = Bytes.toBytes("info");
    // Specify the column name.
    byte[][] qualifiers = { Bytes.toBytes("name"), Bytes.toBytes("gender"), Bytes.toBytes("age"),
        Bytes.toBytes("address") };

    Table table = null;
    try {
      // Instantiate an HTable object.
      table = conn.getTable(tableName);
      List<Put> puts = new ArrayList<Put>();
      // Instantiate a Put object.
      Put put = new Put(Bytes.toBytes("012005000201"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhang San"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("19"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shenzhen, Guangdong"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000202"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Li Wanting"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("23"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shijiazhuang, Hebei"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000203"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Wang Ming"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("26"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Ningbo, Zhejiang"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000204"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Li Gang"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("18"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Xiangyang, Hubei"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000205"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhao Enru"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("21"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shangrao, Jiangxi"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000206"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Chen Long"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("32"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Zhuzhou, Hunan"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000207"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhou Wei"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("29"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Nanyang, Henan"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000208"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Yang Yiwen"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("30"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Kaixian, Chongqing"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000209"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Xu Bing"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("26"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Weinan, Shaanxi"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000210"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Xiao Kai"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("25"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Dalian, Liaoning"));
      puts.add(put);

      // Submit a put request.
      table.put(puts);

      log.info("Put successfully.");
    } catch (IOException e) {
      log.error("Put failed ", e);
    } finally {
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          log.error("Close table failed ", e);
        }
      }
    }
    log.info("Exiting testPut.");
  }
  /**
   * Scan data by secondary index.
   */
  public void testScanDataByIndex() {
    log.info("Entering testScanDataByIndex.");

    Table table = null;
    ResultScanner scanner = null;
    try {
      table = conn.getTable(tableName);

      // Create a filter for indexed column.
      Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
          CompareOp.EQUAL, "Li Gang".getBytes());
      Scan scan = new Scan();
      scan.setFilter(filter);
      scanner = table.getScanner(scan);
      log.info("Scan indexed data.");

      for (Result result : scanner) {
        for (Cell cell : result.rawCells()) {
          log.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
              + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
              + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
              + Bytes.toString(CellUtil.cloneValue(cell)));
        }
      }
      log.info("Scan data by index successfully.");
    } catch (IOException e) {
      log.error("Scan data by index failed ", e);
    } finally {
      if (scanner != null) {
        // Close the scanner object.
        scanner.close();
      }
      try {
        if (table != null) {
          table.close();
        }
      } catch (IOException e) {
        log.error("Close table failed ", e);
      }
    }

    log.info("Exiting testScanDataByIndex.");
  }

  /**
   * Modify a Table
   */
  public void testModifyTable() {
    log.info("Entering testModifyTable.");

    // Specify the column family name.
    byte[] familyName = Bytes.toBytes("education");

    Admin admin = null;
    try {
      // Instantiate an Admin object.
      admin = conn.getAdmin();

      // Obtain the table descriptor.
      HTableDescriptor htd = admin.getTableDescriptor(tableName);

      // Check whether the column family is specified before modification.
      if (!htd.hasFamily(familyName)) {
        // Create the column descriptor.
        HColumnDescriptor hcd = new HColumnDescriptor(familyName);
        htd.addFamily(hcd);

        // Disable the table to get the table offline before modifying
        // the table.
        admin.disableTable(tableName);
        // Submit a modifyTable request.
        admin.modifyTable(tableName, htd);
        // Enable the table to get the table online after modifying the
        // table.
        admin.enableTable(tableName);
      }
      log.info("Modify table successfully.");
    } catch (IOException e) {
      log.error("Modify table failed ", e);
    } finally {
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          log.error("Close admin failed ", e);
        }
      }
    }
    log.info("Exiting testModifyTable.");
  }

  /**
   * Get Data
   */
  public void testGet() {
    log.info("Entering testGet.");

    // Specify the column family name.
    byte[] familyName = Bytes.toBytes("info");
    // Specify the column name.
    byte[][] qualifier = { Bytes.toBytes("name"), Bytes.toBytes("address") };
    // Specify RowKey.
    byte[] rowKey = Bytes.toBytes("012005000201");

    Table table = null;
    try {
      // Create the Configuration instance.
      table = conn.getTable(tableName);

      // Instantiate a Get object.
      Get get = new Get(rowKey);

      // Set the column family name and column name.
      get.addColumn(familyName, qualifier[0]);
      get.addColumn(familyName, qualifier[1]);

      // Submit a get request.
      Result result = table.get(get);

      // Print query results.
      for (Cell cell : result.rawCells()) {
        log.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
            + Bytes.toString(CellUtil.cloneValue(cell)));
      }
      log.info("Get data successfully.");
    } catch (IOException e) {
      log.error("Get data failed ", e);
    } finally {
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          log.error("Close table failed ", e);
        }
      }
    }
    log.info("Exiting testGet.");
  }
  public ResultScanner getScanner(String tableName, String startRowKey, String endRowKey) {
    try {
     // HTable table = new HTable(conf, tableName);
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

  public void testScanData() {
    log.info("Entering testScanData.");

    Table table = null;
    // Instantiate a ResultScanner object.
    ResultScanner rScanner = null;
    try {
      // Create the Configuration instance.
      table = conn.getTable(tableName);

      // Instantiate a Get object.
      Scan scan = new Scan();
      scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

      // Set the cache size.
      scan.setCaching(1000);

      // Submit a scan request.
      rScanner = table.getScanner(scan);

      // Print query results.
      for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
        for (Cell cell : r.rawCells()) {
          log.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
              + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
              + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
              + Bytes.toString(CellUtil.cloneValue(cell)));
        }
      }
      log.info("Scan data successfully.");
    } catch (IOException e) {
      log.error("Scan data failed ", e);
    } finally {
      if (rScanner != null) {
        // Close the scanner object.
        rScanner.close();
      }
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          log.error("Close table failed ", e);
        }
      }
    }
    log.info("Exiting testScanData.");
  }

  public void testSingleColumnValueFilter() {
    log.info("Entering testSingleColumnValueFilter.");

    Table table = null;

    // Instantiate a ResultScanner object.
    ResultScanner rScanner = null;

    try {
      // Create the Configuration instance.
      table = conn.getTable(tableName);

      // Instantiate a Get object.
      Scan scan = new Scan();
      scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

      // Set the filter criteria.
      SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
          Bytes.toBytes("name"), CompareOp.EQUAL, Bytes.toBytes("Xu Bing"));

      scan.setFilter(filter);

      // Submit a scan request.
      rScanner = table.getScanner(scan);

      // Print query results.
      for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
        for (Cell cell : r.rawCells()) {
          log.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
              + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
              + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
              + Bytes.toString(CellUtil.cloneValue(cell)));
        }
      }
      log.info("Single column value filter successfully.");
    } catch (IOException e) {
      log.error("Single column value filter failed ", e);
    } finally {
      if (rScanner != null) {
        // Close the scanner object.
        rScanner.close();
      }
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          log.error("Close table failed ", e);
        }
      }
    }
    log.info("Exiting testSingleColumnValueFilter.");
  }

  public void testFilterList() {
    log.info("Entering testFilterList.");

    Table table = null;

    // Instantiate a ResultScanner object.
    ResultScanner rScanner = null;

    try {
      // Create the Configuration instance.
      table = conn.getTable(tableName);

      // Instantiate a Get object.
      Scan scan = new Scan();
      scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

      // Instantiate a FilterList object in which filters have "and"
      // relationship with each other.
      FilterList list = new FilterList(Operator.MUST_PASS_ALL);
      // Obtain data with age of greater than or equal to 20.
      list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
          CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(new Long(20))));
      // Obtain data with age of less than or equal to 29.
      list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
          CompareOp.LESS_OR_EQUAL, Bytes.toBytes(new Long(29))));

      scan.setFilter(list);

      // Submit a scan request.
      rScanner = table.getScanner(scan);
      // Print query results.
      for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
        for (Cell cell : r.rawCells()) {
          log.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
              + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
              + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
              + Bytes.toString(CellUtil.cloneValue(cell)));
        }
      }
      log.info("Filter list successfully.");
    } catch (IOException e) {
      log.error("Filter list failed ", e);
    } finally {
      if (rScanner != null) {
        // Close the scanner object.
        rScanner.close();
      }
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          log.error("Close table failed ", e);
        }
      }
    }
    log.info("Exiting testFilterList.");
  }

  /**
   * deleting data
   */
  public void testDelete() {
    log.info("Entering testDelete.");

    byte[] rowKey = Bytes.toBytes("012005000201");

    Table table = null;
    try {
      // Instantiate an HTable object.
      table = conn.getTable(tableName);

      // Instantiate an Delete object.
      Delete delete = new Delete(rowKey);

      // Submit a delete request.
      table.delete(delete);

      log.info("Delete table successfully.");
    } catch (IOException e) {
      log.error("Delete table failed ", e);
    } finally {
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          log.error("Close table failed ", e);
        }
      }
    }
    log.info("Exiting testDelete.");
  }

 /* public void dropIndex() {
    log.info("Entering dropIndex.");

    String indexName = "index_name";

    IndexAdmin iAdmin = null;
    try {
      // Instantiate IndexAdmin Object
      iAdmin = new IndexAdmin(conf);

      // Delete Secondary Index
      iAdmin.dropIndex(tableName, indexName);

      log.info("Drop index successfully.");
    } catch (IOException e) {
      log.error("Drop index failed ", e);
    } finally {
      if (iAdmin != null) {
        try {
          // Close Secondary Index
          iAdmin.close();
        } catch (IOException e) {
          log.error("Close admin failed ", e);
        }
      }
    }
    log.info("Exiting dropIndex.");
  }*/

  /**
   * Delete user table
   */
  public void dropTable() {
    log.info("Entering dropTable.");

    Admin admin = null;
    try {
      admin = conn.getAdmin();
      if (admin.tableExists(tableName)) {
        // Disable the table before deleting it.
        admin.disableTable(tableName);

        // Delete table.
        admin.deleteTable(tableName);
      }
      log.info("Drop table successfully.");
    } catch (IOException e) {
      log.error("Drop table failed ", e);
    } finally {
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          log.error("Close admin failed ", e);
        }
      }
    }
    log.info("Exiting dropTable.");
  }

/*  public void grantACL() {
    log.info("Entering grantACL.");

    String user = "huawei";
    String permissions = "RW";

    String familyName = "info";
    String qualifierName = "name";

    Table mt = null;
    Admin hAdmin = null;
    try {
      // Create ACL Instance
      mt = conn.getTable(AccessControlLists.ACL_TABLE_NAME);

      Permission perm = new Permission(Bytes.toBytes(permissions));

      hAdmin = conn.getAdmin();
      HTableDescriptor ht = hAdmin.getTableDescriptor(tableName);

      // Judge whether the table exists
      if (hAdmin.tableExists(mt.getName())) {
        // Judge whether ColumnFamily exists
        if (ht.hasFamily(Bytes.toBytes(familyName))) {
          // grant permission
          AccessControlClient.grant(conn, tableName, user, Bytes.toBytes(familyName),
              (qualifierName == null ? null : Bytes.toBytes(qualifierName)), perm.getActions());
        } else {
          // grant permission
        	AccessControlClient.grant(conn, tableName, user, null, null, perm.getActions());
        }
      }
      log.info("Grant ACL successfully.");
    } catch (Throwable e) {
      log.error("Grant ACL failed ", e);
    } finally {
      if (mt != null) {
        try {
          // Close
          mt.close();
        } catch (IOException e) {
          log.error("Close table failed ", e);
        }
      }

      if (hAdmin != null) {
        try {
          // Close Admin Object
          hAdmin.close();
        } catch (IOException e) {
          log.error("Close admin failed ", e);
        }
      }
    }
    log.info("Exiting grantACL.");
  }*/

  public void testMOBDataRead() {
    log.info("Entering testMOBDataRead.");
    ResultScanner scanner = null;
    Table table = null;
    Admin admin = null;
    try {

      // get table object representing table tableName
      table = conn.getTable(tableName);
      admin = conn.getAdmin();
      admin.flush(table.getName());
      Scan scan = new Scan();
      // get table scanner
      scanner = table.getScanner(scan);
      for (Result result : scanner) {
        byte[] value = result.getValue(Bytes.toBytes("mobcf"), Bytes.toBytes("cf1"));
        String string = Bytes.toString(value);
        log.info("value:" + string);
      }
      log.info("MOB data read successfully.");
    } catch (Exception e) {
      log.error("MOB data read failed ", e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (table != null) {
        try {
          // Close table object
          table.close();
        } catch (IOException e) {
          log.error("Close table failed ", e);
        }
      }
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          log.error("Close admin failed ", e);
        }
      }
    }
    log.info("Exiting testMOBDataRead.");
  }

  public void testMOBDataInsertion() {
    log.info("Entering testMOBDataInsertion.");

    Table table = null;
    try {
      // set row name to "row"
      Put p = new Put(Bytes.toBytes("row"));
      byte[] value = new byte[1000];
      // set the column value of column family mobcf with the value of "cf1"
      p.addColumn(Bytes.toBytes("mobcf"), Bytes.toBytes("cf1"), value);
      // get the table object represent table tableName
      table = conn.getTable(tableName);
      // put data
      table.put(p);
      log.info("MOB data inserted successfully.");

    } catch (Exception e) {
      log.error("MOB data inserted failed ", e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (Exception e1) {
          log.error("Close table failed ", e1);
        }
      }
    }
    log.info("Exiting testMOBDataInsertion.");
  }

/*  public void testCreateMOBTable() {
    log.info("Entering testCreateMOBTable.");

    Admin admin = null;
    try {
      // Create Admin instance
      admin = conn.getAdmin();
      HTableDescriptor tabDescriptor = new HTableDescriptor(tableName);
      HColumnDescriptor mob = new HColumnDescriptor("mobcf");
      // Open mob function
      mob.setMobEnabled(true);
      // Set mob threshold
      mob.setMobThreshold(10L);
      tabDescriptor.addFamily(mob);
      admin.createTable(tabDescriptor);
      log.info("MOB Table is created successfully.");

    } catch (Exception e) {
      log.error("MOB Table is created failed ", e);
    } finally {
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          log.error("Close admin failed ", e);
        }
      }
    }
    log.info("Exiting testCreateMOBTable.");
  }*/
}
