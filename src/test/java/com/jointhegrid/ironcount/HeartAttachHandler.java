package com.jointhegrid.ironcount;

import kafka.message.Message;

public class HeartAttachHandler extends SimpleMessageHandler {

  @Override
  public void handleMessage(Message m) {
    super.handleMessage(m);
    if (this.messageCount.get()%3==0){
      throw new RuntimeException("Heart att att tack. You ouwda know by now");
    }
  }


}
