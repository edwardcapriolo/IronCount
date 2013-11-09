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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columns == null) ? 0 : columns.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Tuple other = (Tuple) obj;
    
    if (columns == null) {
      if (other.columns != null)
        return false;
    } else if (!columns.equals(other.columns))
      return false;
    return true;
  }
  
  public String toString(){
    return this.columns.toString();
  }
}
