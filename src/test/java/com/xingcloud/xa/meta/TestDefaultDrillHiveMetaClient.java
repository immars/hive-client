package com.xingcloud.xa.meta;

import com.xingcloud.meta.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import java.util.List;

public class TestDefaultDrillHiveMetaClient {

  @Test
  public void testCreateDEUTable() throws Exception{
    HiveConf conf = new HiveConf();
    DefaultDrillHiveMetaClient client = new DefaultDrillHiveMetaClient(conf);
    Table deu = TableInfo.newTable();
    String tableName = "deu_age";
    String dbName = "test_xa";
    String userTableName = "user_age";
    String userIndexName = "user_index_age";
    deu.setTableName(tableName);
    deu.setDbName(dbName);
    client.dropTable(dbName, tableName);
    FieldSchema dateField = new FieldSchema("date", "int", "TEXT:8");
    HBaseFieldInfo.setColumnType(deu, dateField, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.TEXT, 8);
    FieldSchema event0Field = new FieldSchema("event0","string", "TEXT");
    HBaseFieldInfo.setColumnType(deu, event0Field, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.WORD, 0);
    FieldSchema event1Field = new FieldSchema("event1","string", "TEXT");
    HBaseFieldInfo.setColumnType(deu, event1Field, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.WORD, 0);    
    FieldSchema event2Field = new FieldSchema("event2","string", "TEXT");
    HBaseFieldInfo.setColumnType(deu, event2Field, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.WORD, 0);
    FieldSchema event3Field = new FieldSchema("event3","string", "TEXT");
    HBaseFieldInfo.setColumnType(deu, event3Field, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.WORD, 0);
    FieldSchema event4Field = new FieldSchema("event4","string", "TEXT");
    HBaseFieldInfo.setColumnType(deu, event4Field, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.WORD, 0);
    FieldSchema uidSampleHashField = new FieldSchema("uhash", "tinyint", "BINARY:1");
    HBaseFieldInfo.setColumnType(deu, uidSampleHashField, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.BINARY, 1);
    FieldSchema uidField = new FieldSchema("uid", "int", "BINARY:4");
    HBaseFieldInfo.setColumnType(deu, uidField, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.BINARY, 4);
    FieldSchema valueField = new FieldSchema("value","bigint", "BINARY:8");
    HBaseFieldInfo.setColumnType(deu, valueField, HBaseFieldInfo.FieldType.cellvalue, "val", "val", HBaseFieldInfo.DataSerType.BINARY, 8);
    FieldSchema timestampField = new FieldSchema("timestamp","bigint", "BINARY:8");  
    HBaseFieldInfo.setColumnType(deu, timestampField, HBaseFieldInfo.FieldType.cversion, "val", "val", HBaseFieldInfo.DataSerType.BINARY, 8);
    deu.getSd().addToCols(dateField);
    deu.getSd().addToCols(event0Field);
    deu.getSd().addToCols(event1Field);
    deu.getSd().addToCols(event2Field);
    deu.getSd().addToCols(event3Field);
    deu.getSd().addToCols(event4Field);
    deu.getSd().addToCols(uidSampleHashField);
    deu.getSd().addToCols(uidField);
    deu.getSd().addToCols(timestampField);
    
    TableInfo.setPrimaryKeyPattern(deu, "${date}${event0}.${event1}.${event2}.${event3}.${event4}\\xFF${uhash}${uid}");    
    client.createTable(deu);
    
    Table user = TableInfo.newTable();
    user.setDbName(dbName);
    user.setTableName(userTableName);
    client.dropTable(dbName, userTableName);
    uidField = new FieldSchema("uid", "int", "BINARY:4");
    HBaseFieldInfo.setColumnType(user, uidField, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.BINARY, 4);
    FieldSchema ref0Field = new FieldSchema("ref0", "string", "TEXT");
    HBaseFieldInfo.setColumnType(user, ref0Field, HBaseFieldInfo.FieldType.cellvalue, "value", "ref0", HBaseFieldInfo.DataSerType.TEXT, 0);
    FieldSchema regTimeField = new FieldSchema("first_login_time", "bigint", "TEXT:14");
    HBaseFieldInfo.setColumnType(user, regTimeField, HBaseFieldInfo.FieldType.cellvalue, "value", "first_login_time", HBaseFieldInfo.DataSerType.TEXT, 14);
    user.getSd().addToCols(uidField);
    user.getSd().addToCols(ref0Field);
    user.getSd().addToCols(regTimeField);
    
    TableInfo.setPrimaryKeyPattern(user, "${uid}");
    client.createTable(user);
    
    
    Table userIndex = TableInfo.newTable();
    userIndex.setDbName(dbName);
    userIndex.setTableName(userIndexName);
    client.dropTable(dbName, userIndexName);
    FieldSchema propNumber = new FieldSchema("propnumber", "tinyint","TEXT");
    HBaseFieldInfo.setColumnType(userIndex, propNumber, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.TEXT, 0);    
    dateField = new FieldSchema("date", "int", "TEXT:8");
    HBaseFieldInfo.setColumnType(userIndex, dateField, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.TEXT, 8);
    valueField = new FieldSchema("value", "binary", "BINARY");
    HBaseFieldInfo.setColumnType(userIndex, valueField, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.BINARY, 8);
    uidField = new FieldSchema("uid", "int", "BINARY:4");
    HBaseFieldInfo.setColumnType(userIndex, uidField, HBaseFieldInfo.FieldType.cqname, "value", null, HBaseFieldInfo.DataSerType.BINARY, 4);
    
    userIndex.getSd().addToCols(propNumber);
    userIndex.getSd().addToCols(dateField);
    userIndex.getSd().addToCols(valueField);
    userIndex.getSd().addToCols(uidField);
    TableInfo.setPrimaryKeyPattern(userIndex, "${propnumber}_${date}_${value}");
    client.createTable(userIndex);
    
    Table table = client.getTable(dbName, tableName);
    printColumns(table);
    printColumns(client.getTable(dbName, userTableName));
    printColumns(client.getTable(dbName, userIndexName));
    
  }

  private void printColumns(Table table) {
    List<KeyPart> pkSequence = TableInfo.getPrimaryKey(table);
    System.out.println("pkSequence = " + pkSequence);
    for(FieldSchema fieldSchema: table.getSd().getCols()){
      HBaseFieldInfo fieldInfo = HBaseFieldInfo.getColumnType(table, fieldSchema);
      System.out.println("fieldInfo = " + fieldInfo);
    }
  }
}
