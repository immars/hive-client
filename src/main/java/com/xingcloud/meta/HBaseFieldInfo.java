package com.xingcloud.meta;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.StringTokenizer;

public class HBaseFieldInfo extends FieldInfo {
  public static enum FieldType {
    rowkey, // this field is part of rowkey.
    cellvalue, // this field is value in a cell (in a column with a qualifier name within a column family)
    cqname, // this field the qualifier name within a column family.
    cversion // this field is the version number of a cell.
  }

  /**
   * data serialization type in hbase.
   * for example, int can be serialized as binary OR text.
   * 255 is serialized to '\xFF' as binary:1, to '\xFF\x00\x00\x00' as binary:4,
   * to '255' as text, to '00000255' as text:8
   */
  public static enum DataSerType {
    BINARY,
    TEXT,
    WORD //special case of TEXT, containing alphabets, numbers and '_' only
  }
  public static final String dim = ":";
  public static final String supdim = ",";
  
  public FieldSchema fieldSchema;
  public FieldType fieldType;
  public String cfName;
  public String cqName;

  
  public DataSerType serType = DataSerType.BINARY;
  
  public int serLength = 0; //<=0 means variable length
  
  public HBaseFieldInfo(FieldSchema fieldSchema, FieldType fieldType, String cfName, String cqName, DataSerType serType, int serLength) {
    this.fieldSchema = fieldSchema;
    this.fieldType = fieldType;
    this.cfName = cfName;
    this.cqName = cqName;
    this.serType = serType;
    this.serLength = serLength;
  }
  
  public static void setColumnType(Table table, HBaseFieldInfo hBaseFieldInfo){
    setColumnType(table, hBaseFieldInfo.fieldSchema, hBaseFieldInfo.fieldType, hBaseFieldInfo.cfName, hBaseFieldInfo.cqName, hBaseFieldInfo.serType, hBaseFieldInfo.serLength);
  }
  public static void setColumnType(Table table, FieldSchema fieldSchema, FieldType fieldType, String cfName, String cqName, DataSerType serType, int serLength){
    String metaSpec = serializeMeta(fieldType, cfName, cqName, serType, serLength);
    setStorageMeta(table, fieldSchema, metaSpec);
  }

  private static String serializeMeta(FieldType fieldType, String cfName, String cqName, DataSerType serType, int serLength) {
    String positionSpec = "";
    
    switch (fieldType){
      case rowkey:
        positionSpec = fieldType.name();
        break;
      case cqname:
        if(null == cfName){
          throw new NullPointerException("cfName null with fieldtype:"+fieldType);
        }
        positionSpec = fieldType.name()+dim+cfName;
        break;
      case cellvalue:
        if(null == cfName || cqName == null){
          throw new NullPointerException("cfName/cqName null with fieldtype:"+fieldType);
        }
        positionSpec = fieldType.name()+dim+cfName+dim+cqName;        
        break;
      case cversion:
        if(null == cfName || cqName == null){
          throw new NullPointerException("cfName/cqName null with fieldtype:"+fieldType);
        }
        positionSpec = fieldType.name()+dim+cfName+dim+cqName;
        break;
      default:
        throw new NullPointerException("cannot deal with fieldtype:"+fieldType);
    }
    String serializationSpec= "";
    if(serLength >0){
      serializationSpec = serType.name()+dim+serLength;
    }else{
      serializationSpec = serType.name();
    }
    return positionSpec+supdim+serializationSpec;
  }

  public static HBaseFieldInfo getColumnType(Table table, FieldSchema fieldSchema){
    String storageMeta = getStorageMeta(table, fieldSchema);
    if(storageMeta == null || "".equals(storageMeta)){
      return null;
    }
    String[] tokens = storageMeta.split(supdim);
    if(tokens.length != 2){
      throw new NullPointerException("illegal storage meta:"+storageMeta);
    }
    String positionSpec = tokens[0];
    String serializationSpec = tokens[1];
    HBaseFieldInfo ret = new HBaseFieldInfo(fieldSchema, null, null, null, null, 0);
    parsePositionSpec(table, ret, positionSpec);
    parseSerializationSpec(table, ret, serializationSpec);
    return ret;
  }

  private static void parseSerializationSpec(Table table, HBaseFieldInfo hBaseFieldInfo, String serializationSpec) {
    String[] tokens = serializationSpec.split(dim);
    DataSerType serType = DataSerType.valueOf(tokens[0]);
    hBaseFieldInfo.serType = serType;
    switch (tokens.length){
      case 1:
        hBaseFieldInfo.serLength = 0;
        break;
      case 2:
        hBaseFieldInfo.serLength = Integer.valueOf(tokens[1]);
        break;
      default:
        throw new NullPointerException("bad serialization spec!"+serializationSpec);
    }
  }

  private static void parsePositionSpec(Table table, HBaseFieldInfo hBaseFieldInfo, String positionSpec) {
    String[] tokens = positionSpec.split(dim);
    FieldType fieldType = FieldType.valueOf(tokens[0]);
    hBaseFieldInfo.fieldType=fieldType;
    switch (fieldType){
      case rowkey:
        if(tokens.length!=1){
          throw new NullPointerException("wrong token number for rowkey:"+positionSpec);
        }
        return;
      case cqname:
        if(tokens.length!=2){
          throw new NullPointerException("wrong token number for cqname:"+positionSpec);
        }
        hBaseFieldInfo.cfName = tokens[1];
        return;
      case cellvalue:
        if(tokens.length!=3){
          throw new NullPointerException("wrong token number for cellvalue:"+positionSpec);
        }
        hBaseFieldInfo.cfName = tokens[1];
        hBaseFieldInfo.cqName = tokens[2];
        return;
      case cversion:
        if(tokens.length!=3){
          throw new NullPointerException("wrong token number for cversion:"+positionSpec);
        }
        hBaseFieldInfo.cfName = tokens[1];
        hBaseFieldInfo.cqName = tokens[2];        
        return;
      default:
        throw new NullPointerException("cannot deal with fieldtype:"+fieldType);
    }
  }

  @Override
  public String toString() {
    return "HBaseFieldInfo{" +
      "fieldSchema=" + fieldSchema +
      ", "+ serializeMeta(this.fieldType, this.cfName, this.cqName, this.serType, this.serLength) +" }";
  }
}
