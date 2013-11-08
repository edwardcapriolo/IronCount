package com.jointhegrid.ironcount.model;

public abstract class ICollector {
  public abstract void emit(Tuple source, Tuple out);
}
