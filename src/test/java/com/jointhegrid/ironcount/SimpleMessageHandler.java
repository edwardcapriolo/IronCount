/*
Copyright 2011 Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
    System.out.println("handlerId:"+ myId+" message:"+ IronIntegrationTest.getMessage(m));
  }

  @Override
  public void setWorkload(Workload w) {
    
  }

  @Override
  public void setWorkerThread(WorkerThread wt) {
  }

  @Override
  public void stop() {
  }
 
}
