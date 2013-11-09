package com.jointhegrid.ironcount.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.cassandra.utils.Pair;

public class Collector extends ICollector{

  private ArrayBlockingQueue<Pair<Tuple,Tuple>> collected;

  public Collector(){
    collected = new ArrayBlockingQueue<Pair<Tuple,Tuple>>(4000);
  }
  
  @Override
  public void emit(Tuple source, Tuple out) {
    collected.add(new Pair<Tuple,Tuple>(source,out));
  }

  public Pair<Tuple,Tuple> take() throws InterruptedException{
    return collected.take();
  }
  
  public Pair<Tuple,Tuple> peek() throws InterruptedException{
    return collected.peek();
  }
  
}
