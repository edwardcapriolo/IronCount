package com.jointhegrid.ironcount;

import java.util.concurrent.atomic.AtomicInteger;
import kafka.message.Message;

public class SimpleMessageHandler implements MessageHandler{

  public static AtomicInteger messageCount;
  static {
    messageCount= new AtomicInteger(0);
  }
  public SimpleMessageHandler(){

  }

  @Override
  public void handleMessage(Message m) {
    messageCount.addAndGet(1);
  }

  @Override
  public void setWorkload(Workload w) {
    
  }

}
