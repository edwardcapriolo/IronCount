package com.jointhegrid.ironcount.model;

import java.util.List;
import java.util.Map;

public abstract class Feed {

  private String name;
  protected Map properties;
  
  public Feed(Map properties){
    this.properties = properties;
  }
  
  public abstract List<FeedPartition> getFeedPartitions();
}

