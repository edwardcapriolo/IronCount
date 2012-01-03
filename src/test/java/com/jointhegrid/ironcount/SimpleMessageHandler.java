package com.jointhegrid.ironcount;

import java.util.concurrent.atomic.AtomicInteger;
import kafka.message.Message;

public class SimpleMessageHandler implements MessageHandler{

  public static AtomicInteger messageCount;
  public static AtomicInteger handlerCount;
  public int myId;

  static {
    messageCount= new AtomicInteger(0);
    handlerCount = new AtomicInteger(0);
  }
  
  public SimpleMessageHandler(){
    myId= handlerCount.addAndGet(1);
  }

  @Override
  public void handleMessage(Message m) {
    messageCount.addAndGet(1);
  }

  @Override
  public void setWorkload(Workload w) {
    
  }

}
