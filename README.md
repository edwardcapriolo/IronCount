IronCount
=============

IronCount provides a framework to manage consumers of Kafka message queues across multiple nodes.

Usage
-----

package com.jointhegrid.ironcount.mockingbird;

import com.jointhegrid.ironcount.MessageHandler;
import com.jointhegrid.ironcount.WorkerThread;
import com.jointhegrid.ironcount.Workload;
import kafka.message.Message;

public class MessageHandlerExt implements MessageHandler{

  public MessageHandlerExt(){}

  @Override
  public void setWorkload(Workload w) {
  }

  @Override
  public void handleMessage(Message m) {
  }

  @Override
  public void setWorkerThread(WorkerThread wt) {
  }
}

