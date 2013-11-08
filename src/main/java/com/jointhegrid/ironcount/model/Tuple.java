package com.jointhegrid.ironcount.model;

import java.util.HashMap;
import java.util.Map;
 
public class Tuple {
  private Map<String, Object> columns;
  
  public Tuple(){
    columns = new HashMap<String,Object>();
  }
  
  public void setField(String name, Object value){
    columns.put(name, value);
  }
  public Object getField(String name){
    return columns.get(name);
  }
}
