package com.jointhegrid.ironcount.eventtofile;

import com.jointhegrid.ironcount.MessageHandler;
import com.jointhegrid.ironcount.WorkerThread;
import com.jointhegrid.ironcount.Workload;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import kafka.message.Message;
import org.apache.log4j.Logger;

public class MessageToFileHandler implements MessageHandler {

  final static Logger logger = Logger.getLogger(MessageToFileHandler.class.getName());
  FileWriter fw ;
  
  public MessageToFileHandler(){
    try {
      fw = new FileWriter(new File("/tmp/abc"));
    } catch (IOException ex) {
      logger.error(ex);
    }
  }

  @Override
  public void setWorkload(Workload w) {

  }

  @Override
  public void handleMessage(Message m) {
    String s = getMessage(m);
    try {
      fw.write(s+"\n");
      fw.flush();
    } catch (IOException ex) {
     logger.error(ex);
    }
  }

  @Override
  public void setWorkerThread(WorkerThread wt) {

  }

  @Override
  public void finalize () throws Throwable{
    super.finalize();
    try {
      fw.close();
    } catch (IOException ex) {
      logger.error(ex);
    }
  }

    public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}