package com.jointhegrid.ironcount.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

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
    Driver root = new Driver(getPart(), minus1Operator());
    root.initialize();
    DriverNode child = new DriverNode(times2Operator(), new CollectorProcessor());
    root.getDriverNode().addChild(child);
    
    Thread t = new Thread(root);
    t.run();
    t.join();

    List<Tuple> expected = new ArrayList<Tuple>();
    for (int i = 0; i < 9; i++) {
      Tuple tup = new Tuple();
      tup.setField("x", (i - 1) * 2);
      expected.add(tup);
    }
    assertExpectedPairs(child, expected);
    
  }

  public void assertExpectedPairs(DriverNode finalNode, List<Tuple> expected) throws InterruptedException {
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertNotNull(finalNode.getCollectorProcessor().collector.peek());
      Tuple got = finalNode.getCollectorProcessor().collector.take().right;
      Assert.assertTrue("element "+i+" comparing " + expected.get(i) + " " + got, expected.get(i).equals(got));
    }
    Assert.assertNull(finalNode.getCollectorProcessor().collector.peek());
  }
  
  @Test
  public void compareTuple(){
    Tuple t = new Tuple();
    t.setField("x", -2);
    Tuple s = new Tuple();
    s.setField("x", -2);
    
    Assert.assertTrue(t.equals(s));
  }
  
  
  private Operator  times2Operator(){
    return new Operator(){
      @Override
      public void handleTuple(Tuple t) {
        Tuple tnew = new Tuple();
        tnew.setField("x", ((Integer) t.getField("x")).intValue() * 2);
        collector.emit(t, tnew);
      }
    };

  }
  
  private Operator minus1Operator(){
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


/*
@Test
public void mapEquals(){
  HashMap m = new HashMap();
  m.put("a","b");
  
  HashMap n = new HashMap();
  n.put("b","c");
  
  HashMap o = new HashMap();
  o.put("a","b");
  
  Assert.assertEquals(m, o);

  
  Assert.assertTrue( m.equals(o));
  Assert.assertTrue( n.equals(o));
}
*/ 