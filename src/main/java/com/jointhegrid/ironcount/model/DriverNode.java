package com.jointhegrid.ironcount.model;

import java.util.ArrayList;
import java.util.List;

public class DriverNode {

  private Operator operator;
  private CollectorProcessor collectorProcessor;
  private Thread thread ;
  private List<DriverNode> children ;
  
  public DriverNode(Operator operator, CollectorProcessor cp){
    this.operator = operator;
    this.collectorProcessor = cp;
    operator.setCollector(cp.collector);
    children = new ArrayList<DriverNode>();
  }
  
  /**
   * initialize driver node and all children of the node
   */
  public void initialize(){
    thread = new Thread(collectorProcessor);
    thread.start();
    for (DriverNode dn : this.children){
      dn.initialize();
    }
  }
  
  /**
   * Method adds ad child data node and bind the collect processor
   * of this node to the operator of the next node
   * @param dn
   */
  public void addChild(DriverNode dn){
    collectorProcessor.children.add(dn.operator);
    this.children.add(dn);
  }
  
  public DriverNode withChild(DriverNode dn){
    addChild(dn);
    return this;
  }

  public Operator getOperator() {
    return operator;
  }

  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  public CollectorProcessor getCollectorProcessor() {
    return collectorProcessor;
  }

  public void setCollectorProcessor(CollectorProcessor collectorProcessor) {
    this.collectorProcessor = collectorProcessor;
  }
  
  
}
