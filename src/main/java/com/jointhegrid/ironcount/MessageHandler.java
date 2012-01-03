package com.jointhegrid.ironcount;

import kafka.message.Message;

public interface MessageHandler {
  public void setWorkload(Workload w);
  public void handleMessage(Message m) ;
}
