package com.xingcloud.meta;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;

public class PrimaryKeyPattern {
  
  public static List<KeyPart> parse(Table table, CharIterator pk) {
    int state = 0;//0 out of ${}; 1 inside ${}
    List<FieldSchema> cols = table.getSd().getCols();    
    List<KeyPart> ret = new ArrayList<KeyPart>();
    StringBuilder sb = new StringBuilder();
    loop:
    for (;pk.hasNext();) {
      char c = pk.next();
      switch (c){
        case '$':
          if(state == 0 && pk.hasNext() && pk.pickNext(0)=='{'){
            String constant = sb.toString();
            if(!constant.isEmpty()){
              ret.add(KeyPart.constantKeyPart(constant));
            }
            state = 1;
            pk.next();//bypass {
            sb = new StringBuilder();
            continue;
          }else{
            sb.append(c);
          }
          break;
        case '}':
          if(state == 1){
            String colName = sb.toString();
            sb = new StringBuilder();
            if(colName.isEmpty()){
              throw new NullPointerException("empty column name!");
            }
            FieldSchema thisField = null;
            for(FieldSchema fieldSchema:cols){
              if(fieldSchema.getName().equals(colName)){
                thisField = fieldSchema;
                break;
              }
            }
            if(thisField == null){
              throw new NullPointerException("column:"+colName+" not found!");
            }
            ret.add(KeyPart.fieldKeyPart(thisField));
            state=0;
          }else{
            sb.append(c);
          }
          break;
        case '[':
          if(state == 1){
            throw new NullPointerException("illegal character in column name:"+c);
          }
          String constant = sb.toString();
          sb = new StringBuilder();
          if(!constant.isEmpty()){
            ret.add(KeyPart.constantKeyPart(constant));
          }
          List<KeyPart> subGroup = parse(table, pk);
          if(subGroup.size()==0){
            throw new NullPointerException("empty optional group!");
          }
          ret.add(KeyPart.optionalGroupKeyPart(subGroup));
          break;
        case ']': // end of current pattern
          break loop;
        default:
            sb.append(c);
      }
    }
    if(sb.length()!=0){
      if(state == 1){
        throw new NullPointerException("col spec not finished:"+sb.toString());
      }
      ret.add(KeyPart.constantKeyPart(sb.toString()));
    }
    return ret;    
  }

  public static List<KeyPart> parse(Table table, String pkString) {
    return parse(table, new CharIterator(pkString.toCharArray(), 0));

  }
  
  static class CharIterator{
    char[] chars;
    int curpos;

    CharIterator(char[] chars, int curpos) {
      this.chars = chars;
      this.curpos = curpos;
    }
    
    boolean hasNext(){
      return curpos < chars.length;
    }
    char next(){
      return chars[curpos++];
    }
    
    char pickNext(int i){
      return chars[curpos+i];
    }
    char get(int i){
      return chars[i];
    }
    void setIndex(int i){
      this.curpos = i;
    }
    int size(){
      return chars.length;
    }
  }
}
