package com.xingcloud.meta;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import java.util.*;

public class MetaClient {

  public static void main(String[] args) throws Exception {
//    testCreateIndex("mytable2");
    testDeleteTable("mytable3");
    //testCreateTable2();
    
  }

  private static void testCreateTable2() throws HiveException {
    Hive hive = Hive.get();
    hive.createTable("mytable3", new ArrayList<String>(), new ArrayList<String>(), org.apache.hadoop.mapred.TextInputFormat.class, org.apache.hadoop.mapred.TextOutputFormat.class, 0, new ArrayList<String>());
  }

  public static void testDeleteTable(String tableName) throws Exception{
    HiveConf conf = new HiveConf();
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    client.dropTable("test_xa",tableName);
  }

  
  public static void testGetTable(String tableName) throws Exception{
    HiveConf conf = new HiveConf();
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    System.out.println(client.getAllDatabases());
    Database database = client.getDatabase("test_xa");
    Table table = new Table();//client.getTable("test_xa", "f");
    table.setDbName("test_xa");
    table.setTableName(tableName);
    table = client.getTable("test_xa", "mytable");
    System.out.println("table = " + table);    
    
  }
  
  public static void testCreateTable(String tableName) throws TException {
    HiveConf conf = new HiveConf();
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    Database database = client.getDatabase("test_xa");
    Table table = new Table();//client.getTable("test_xa", "f");
    table.setDbName("test_xa");
    table.setTableName("mytable");
    table = client.getTable("test_xa", "mytable");
    System.out.println("table = " + table);
    Table table2 = new Table();
    table2.setTableName(tableName);
    table2.setDbName("test_xa");
    StorageDescriptor sd = new StorageDescriptor();
    FieldSchema fieldSchema = new FieldSchema("uid","int", "it is my test uid");
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put("serialization.format", "1");
    serdeInfo.setParameters(parameters);
    SkewedInfo skewedInfo = new SkewedInfo(new ArrayList<String>(), new ArrayList<List<String>>(), new HashMap<List<String>, String>());
    sd.setSkewedInfo(skewedInfo);
    sd.setSerdeInfo(serdeInfo);
    sd.addToCols(fieldSchema);
    sd.setLocation("hdfs://localhost:9000/user/hive/warehouse/test_xa.db/" + tableName);
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    sd.setBucketCols(new ArrayList<String>());
    sd.setSortCols(new ArrayList<Order>());
    sd.setParameters(new HashMap<String, String>());
    table2.setSd(sd);
    table2.setPartitionKeys(new ArrayList<FieldSchema>());
    table2.setTableType("EXTERNAL_TABLE");
    
    Map<String, String> table2Params = new HashMap<String, String>();
    table2Params.put("transient_lastDdlTime", "" + System.currentTimeMillis());
    table2.setParameters(table2Params);
    System.out.println("table2 = " + table2);
    client.createTable(table2);
    client.close();
    
  }

  public static void testCreateIndex()throws Exception {
    HiveConf conf = new HiveConf();
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    Index index = new Index();
    
    index.setDbName("test_xa");
    index.setIndexName("mytable2_pk");
    index.setOrigTableName("mytable2");
    index.setIndexHandlerClass("primarykey");
    client.createIndex(index,null);
  }
  
}
