package com.jointhegrid.ironcount.model;

import java.util.List;
import java.util.Map;

public abstract class Operator {

  protected Map properties;
  protected ICollector collector;
  
  public void setProperties(Map properties){
    this.properties = properties;
  }
  
  public abstract void handleTuple(Tuple t);
  
  public void setCollector(ICollector i){
    this.collector = i;
  }
}
