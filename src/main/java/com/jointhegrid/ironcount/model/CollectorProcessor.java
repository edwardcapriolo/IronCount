package com.jointhegrid.ironcount.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.Pair;

class CollectorProcessor implements Runnable {
  Collector collector;
  List<Operator> children;
  boolean goOn = true;
  
  public CollectorProcessor(){
    children = new ArrayList<Operator>();
    collector = new Collector();
  }
  
  public void run(){
    while(goOn){
      try {
        Pair<Tuple,Tuple> pair = collector.take();
        for (Operator o: children){
          o.handleTuple(pair.right);
        }
      } catch (InterruptedException e) {       
        e.printStackTrace();
      }
    }
  }
}