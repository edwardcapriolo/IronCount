package com.jointhegrid.ironcount.model;

import java.util.List;

public abstract class Operator {

  protected ICollector collector;
  
  public abstract void handleTuple(Tuple t);
  
  public void setCollector(ICollector i){
    this.collector = i;
  }
}
