package com.jointhegrid.ironcount.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class TestFeed {

  @Test
  public void testFeed(){
    Map prop = new HashMap();
    int expectedPartitions = 5;
    int expectedRows = 1000;
    prop.put("number.of.partitions", expectedPartitions);
    prop.put("number.of.rows", expectedRows);
    PartitionedFeed pf = new PartitionedFeed(prop);
    List<FeedPartition> parts = pf.getFeedPartitions();
    Assert.assertEquals(expectedPartitions, parts.size());
    Tuple t = new Tuple();
    
    parts.get(0).next(t);
    Assert.assertEquals( t.getField("x"), 0);
    parts.get(0).next(t);
    Assert.assertEquals(t.getField("x"), 1);
    parts.get(0).next(t);
    Assert.assertEquals(t.getField("x"), 2);

    parts.get(0).next(t);
    Assert.assertEquals(t.getField("x"), 0);
    parts.get(0).next(t);
    Assert.assertEquals(t.getField("x"), 1);
  }
}

class PartitionedFeed extends Feed {
  int numberOfPartitions;
  int numberOfRows;
  
  public PartitionedFeed(Map properties){
    super(properties);
    numberOfPartitions = (Integer) super.properties.get("number.of.partitions");
    numberOfRows = (Integer) super.properties.get("number.of.rows");
  }

  @Override
  public List<FeedPartition> getFeedPartitions() {
    List<FeedPartition> res= new ArrayList<FeedPartition>();
    for (int i=0;i<numberOfPartitions;i++){
      StockFeedPartition sf = new StockFeedPartition(this);
      sf.setPartitionId(i);
      res.add(sf);
    }
    return res;
  }
}

class StockFeedPartition extends FeedPartition {

  private int partitionId;
  
  private int current = 0;
  private int max = 10;
  
  public StockFeedPartition(Feed f) {
    super(f);
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {                                                                                                                                                              
    this.partitionId = partitionId;
  }

  @Override
  public boolean next(Tuple t) {
    t.setField("x",new Integer(current++));
    return current<max;
  }
    
}