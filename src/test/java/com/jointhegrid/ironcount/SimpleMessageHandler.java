package com.jointhegrid.ironcount;

import kafka.message.Message;

public class SimpleMessageHandler implements MessageHandler{

  public static int messageCount;
  public SimpleMessageHandler(){

  }

  @Override
  public void handleMessage(Message m) {
    System.err.println(IntegrationTest.getMessage(m));
    messageCount++;
  }

  @Override
  public void setWorkload(Workload w) {
    
  }

}
