package com.jointhegrid.ironcount.model;



public class Driver implements Runnable {
  private FeedPartition fp;
  private DriverNode driverNode;

  public Driver(FeedPartition fp, Operator operator){
    this.fp = fp;
    CollectorProcessor cp = new CollectorProcessor();
    driverNode = new DriverNode(operator, cp);
  }
  
  public void initialize(){
    driverNode.initialize();
  }
  
  /**
   * Begin processing the feed in a thread
   */
  public void run(){
    Tuple t = new Tuple();
    while (fp.next(t)){
      System.out.println(t);
      driverNode.getOperator().handleTuple(t);
      t = new Tuple();
    }
  }

  public DriverNode getDriverNode() {
    return driverNode;
  }

  public void setDriverNode(DriverNode driverNode) {
    this.driverNode = driverNode;
  }
  
}