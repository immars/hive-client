package com.xingcloud.meta;
import org.apache.hadoop.hive.metastore.api.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class TableInfo {
  public static final String PRIMARY_KEY = "primarykey.columns";
  public static final String PK_DELIMITER = "primarykey.delimiter";
  public static final String STORAGE_ENGINE = "drill.se";
  
  public static final String SE_HBASE = "hbase";
  
  
  public static List<KeyPart> getPrimaryKey(Table table) {
    String pkString = getPrimaryKeyPattern(table); 
    if(pkString == null || "".equals(pkString)){
      return null;
    }
    return PrimaryKeyPattern.parse(table, pkString);
  }


  public static String getPrimaryKeyPattern(Table table) {
    return table.getSd().getSerdeInfo().getParameters() == null? null: table.getSd().getSerdeInfo().getParameters().get(PRIMARY_KEY);    
  }
  
  
  public static void setPrimaryKeyPattern(Table table, String primaryKeyPattern) {
    String oldKey = table.getSd().getSerdeInfo().getParameters().get(PRIMARY_KEY);
    table.getSd().getSerdeInfo().putToParameters(PRIMARY_KEY, primaryKeyPattern);
    try{
      getPrimaryKey(table);
    }catch(Exception e){
      table.getSd().getSerdeInfo().putToParameters(PRIMARY_KEY, oldKey);     
      throw new RuntimeException(e);
    }
  }

  public static void setPrimaryKey(Table table, List<FieldSchema> fieldSchemas) {
    StringBuilder sb = new StringBuilder();
    if (fieldSchemas != null){
      for (int i = 0; i < fieldSchemas.size(); i++) {
        FieldSchema fieldSchema = fieldSchemas.get(i);
        sb.append(fieldSchema.getName());
        if(i != fieldSchemas.size() - 1){
          sb.append(",");
        }
      }
    }
    table.getSd().getSerdeInfo().putToParameters(PRIMARY_KEY, sb.toString());    
  }
  
  public static void setStorageEngine(Table table, String se){
    table.getParameters().put(STORAGE_ENGINE, se);
  }
  
  public static String getStorageEngine(Table table){
    return table.getParameters().get(STORAGE_ENGINE);
  }
  
  public static Table newTable() {
    Table table2 = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put("serialization.format", "1");
    serdeInfo.setParameters(parameters);
    SkewedInfo skewedInfo = new SkewedInfo(new ArrayList<String>(), new ArrayList<List<String>>(), new HashMap<List<String>, String>());
    sd.setSkewedInfo(skewedInfo);
    sd.setSerdeInfo(serdeInfo);
    sd.setCols(new ArrayList<FieldSchema>());
//    sd.setLocation("hdfs://localhost:9000/user/hive/warehouse/test_xa.db/");// need a place
    sd.setLocation("");// need a place
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");//y
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");//y
    sd.setBucketCols(new ArrayList<String>());
    sd.setSortCols(new ArrayList<Order>());
    sd.setParameters(new HashMap<String, String>());
    table2.setSd(sd);//y
    table2.setPartitionKeys(new ArrayList<FieldSchema>());//y
    table2.setTableType("EXTERNAL_TABLE");
    
    Map<String, String> table2Params = new HashMap<String, String>();
//    table2Params.put("transient_lastDdlTime", "" + System.currentTimeMillis());
    table2.setParameters(table2Params);
    return table2;
  }
}
