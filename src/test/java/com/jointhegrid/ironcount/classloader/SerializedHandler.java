package com.jointhegrid.ironcount.classloader;

import com.jointhegrid.ironcount.manager.Workload;
import com.jointhegrid.ironcount.manager.MessageHandler;
import com.jointhegrid.ironcount.manager.WorkerThread;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.message.Message;

public class SerializedHandler implements MessageHandler {

  public static AtomicInteger messageCount;
  public static AtomicInteger handlerCount;
  public int myId;

  static {
    messageCount = new AtomicInteger(0);
    handlerCount = new AtomicInteger(0);
  }

  public SerializedHandler() {
    myId = handlerCount.addAndGet(1);
  }

  @Override
  public void handleMessage(Message m) {
    messageCount.addAndGet(1);
    System.out.println("handlerId:" + myId + " message:" + getMessage(m));
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

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
