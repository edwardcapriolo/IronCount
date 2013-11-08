package com.jointhegrid.ironcount.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class TestDriver {

  private FeedPartition getPart(){
    @SuppressWarnings("rawtypes")
    Map prop = new HashMap();
    int expectedPartitions = 5;
    int expectedRows = 1000;
    prop.put("number.of.partitions", expectedPartitions);
    prop.put("number.of.rows", expectedRows);
    PartitionedFeed pf = new PartitionedFeed(prop);
    List<FeedPartition> parts = pf.getFeedPartitions();
    return parts.get(0);
  }
  
  @Test
  public void aTest() throws InterruptedException {
    Driver root = new Driver(getPart(), trimOperator());
    DriverNode child = new DriverNode(ucaseOperator(), new CollectorProcessor());
    root.getDriverNode().addChild(child);
    
    Thread t = new Thread(root);
    t.run();
    t.join();
    
    
  }
  
  private Operator  ucaseOperator(){
    return new Operator(){
      @Override
      public void handleTuple(Tuple t) {
        Tuple tnew = new Tuple();
        tnew.setField("x", ((Integer) t.getField("x")).intValue() * 2);
        collector.emit(t, tnew);
      }
    };

  }
  
  private Operator trimOperator(){
    return new Operator(){
      @Override
      public void handleTuple(Tuple t) {
        Tuple tnew = new Tuple();
        tnew.setField("x", ((Integer) t.getField("x")).intValue()-1 );
        collector.emit(t, tnew);
      }
    };
  }
  
}
