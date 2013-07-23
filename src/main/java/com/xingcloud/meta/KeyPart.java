package com.xingcloud.meta;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

public class KeyPart {
  static enum Type{
    field, //this part of rowkey corresponding to a field
    constant, // this part of rowkey is a constant/separator between fields
    optionalgroup // this part of rowkey may or may not exists
  }
  
  public Type type;
  
  private FieldSchema field;
  private String constant;  
  private byte[] serializedConstant;
  
  private List<KeyPart> optionalGroup;
  
  public KeyPart(Type type, FieldSchema field, String constant) {
    this(type, field, constant, null);
  }

  public KeyPart(Type type, FieldSchema field, String constant, List<KeyPart> optionalGroup) {
    this.type = type;
    this.field = field;
    this.constant = constant;
    this.optionalGroup = optionalGroup;
  }

  public static KeyPart optionalGroupKeyPart(List<KeyPart> optionalGroup){
    return new KeyPart(Type.optionalgroup, null, null, optionalGroup);
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
    return constant;
  }
  
  public byte[] getSerializedConstant(){
    if(this.serializedConstant == null){
      serializedConstant = escape(constant);
    }
    return serializedConstant;
  }


  public List<KeyPart> getOptionalGroup() {
    return optionalGroup;
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

  public void setConstant(String constant) {
    serializedConstant = null;
    this.constant = constant;
  }

  @Override
  public String toString() {
    switch (type){
      case constant:
        return "KeyPart{'"+constant+"'}";
      case field:
        return "KeyPart{`"+field+"`}";        
      default:
        return "KeyPart"+optionalGroup.toString();
    }
  }
  
}
