package com.xingcloud.meta;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.*;

import java.util.HashMap;
import java.util.Map;

/**
 * save column mapping info into table parameters.
 * for each column, the SE-dependent meta info stored in serde parameters, 
 * keyed as "colstoragemeta.$(columnName)"
 * 
 * this meta may contain information about how logical columns are mapped 
 * to underlying SE.
 * 
 * for example, for hbase SE, a column can be mapped to 
 * part of rowkey, a cell value, a column qualifier, a version number
 */
public class FieldInfo {
  
  public static final String STORAGE_META_PREFIX = "colstoragemeta.";


  /**
   * provide additional interface to store drill specific meta information
   */
  
  public static String getStorageMeta(Table table, FieldSchema fieldSchema) {
    return table.getSd().getSerdeInfo().getParameters().get(STORAGE_META_PREFIX+fieldSchema.getName());
  }

  public static void setStorageMeta(Table table, FieldSchema fieldSchema, String storageMeta) {
    table.getSd().getSerdeInfo().putToParameters(STORAGE_META_PREFIX+fieldSchema.getName(), storageMeta);
  }

}
