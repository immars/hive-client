package com.xingcloud.meta;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultDrillHiveMetaClient extends HiveMetaStoreClient {
  public DefaultDrillHiveMetaClient(HiveConf conf) throws MetaException {
    super(conf);
  }

  public DefaultDrillHiveMetaClient(HiveConf conf, HiveMetaHookLoader hookLoader) throws MetaException {
    super(conf, hookLoader);
  }

  /**
   * make life easier when creating tables
   * @param tbl
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  @Override
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    StorageDescriptor sd = tbl.getSd();
    if(sd.getSkewedInfo() == null){
      sd.setSkewedInfo(new SkewedInfo(new ArrayList<String>(), new ArrayList<List<String>>(), new HashMap<List<String>, String>()));      
    }
    if(sd.getSerdeInfo() == null){
      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      Map<String, String> parameters = new HashMap<String, String>();
      parameters.put("serialization.format", "1");
      serdeInfo.setParameters(parameters);
      sd.setSerdeInfo(serdeInfo);
    }
    if(sd.getLocation() == null){
      sd.setLocation("");      
    }
    if(sd.getInputFormat() == null){
      sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    }
    if(sd.getOutputFormat() == null){
      sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    }
    if(sd.getBucketCols() == null){
      sd.setBucketCols(new ArrayList<String>());
    }
    if(sd.getSortCols() == null){
     sd.setSortCols(new ArrayList<Order>());
    }
    if(sd.getParameters() == null){
      sd.setParameters(new HashMap<String, String>());
    }
    if(tbl.getPartitionKeys()==null){
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    }
    if(tbl.getTableType()==null){
      tbl.setTableType("EXTERNAL_TABLE");      
    }
    super.createTable(tbl);    
  }
  
}
