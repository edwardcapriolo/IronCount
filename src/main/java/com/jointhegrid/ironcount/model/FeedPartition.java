package com.jointhegrid.ironcount.model;

public abstract class FeedPartition {

  public FeedPartition(Feed f){
    
  }
  
  public void initialize(){
    
  }
  
  public abstract boolean next(Tuple t);
  
  public void close(){
    
  }
}
