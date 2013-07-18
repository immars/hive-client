package com.xingcloud.meta;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class KeyPart {
  static enum Type{
    field,
    constant
  }
  
  public Type type;
  
  private FieldSchema field;
  private String constant;

  private byte[] serializedConstant;
  
  public KeyPart(Type type, FieldSchema field, String constant) {
    this.type = type;
    this.field = field;
    this.constant = constant;
  }

  public static KeyPart fieldKeyPart(FieldSchema fieldSchema){
    return new KeyPart(Type.field, fieldSchema, null);
  }
  
  public static KeyPart constantKeyPart(String constant){
    return new KeyPart(Type.constant, null, constant);
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public FieldSchema getField() {
    return field;
  }

  public void setField(FieldSchema field) {
    this.field = field;
  }

  public String getConstant() {
    serializedConstant = null;
    return constant;
  }
  
  public byte[] getSerializedConstant(){
    if(this.serializedConstant == null){
      serializedConstant = escape(constant);
    }
    return serializedConstant;
  }

  /**
   * escape '\xAA' like sequence
   * @param constant
   * @return
   */
  private static byte[] escape(String constant) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int i = 0; i < constant.length(); i++) {
      char c = constant.charAt(i);
      if(c == '\\' && constant.length()>i+3
        && constant.charAt(i+1) =='x'){
        char h = constant.charAt(i+2);
        char l = constant.charAt(i+3);
        baos.write(Integer.parseInt(constant.substring(i+2, i+4), 16));
        i+=3;
      }else{
        baos.write(c);
      }
    }
    return baos.toByteArray();
  }

  
  private static short toInt(char c) {
    
    return 0;  //TODO method implementation
  }

  public void setConstant(String constant) {
    this.constant = constant;
  }

  @Override
  public String toString() {
    switch (type){
      case constant:
        return "KeyPart{'"+constant+"'}";
      default:
        return "KeyPart{`"+field+"`}";        
    }
  }
  
}
