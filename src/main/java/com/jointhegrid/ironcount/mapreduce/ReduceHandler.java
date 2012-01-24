/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jointhegrid.ironcount.mapreduce;

import com.jointhegrid.ironcount.MessageHandler;
import com.jointhegrid.ironcount.WorkerThread;
import com.jointhegrid.ironcount.Workload;
import java.nio.ByteBuffer;
import kafka.message.Message;

/**
 *
 * @author edward
 */
public class ReduceHandler implements MessageHandler {

  @Override
  public void setWorkload(Workload w) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void handleMessage(Message m) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setWorkerThread(WorkerThread wt) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
