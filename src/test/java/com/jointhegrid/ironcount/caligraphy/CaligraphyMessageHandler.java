package com.jointhegrid.ironcount.caligraphy;

import com.jointhegrid.ironcount.manager.MessageHandler;
import com.jointhegrid.ironcount.manager.WorkerThread;
import com.jointhegrid.ironcount.manager.Workload;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CaligraphyMessageHandler implements MessageHandler {

  private Workload w;
  private WorkerThread wt;
  private Map<String,Writer> outs;
  FileSystem fs;

  public CaligraphyMessageHandler(){
    outs=new HashMap<String,Writer>();
  }

  @Override
  public void setWorkload(Workload w) {
    this.w=w;
  }

  @Override
  public void handleMessage(MessageAndMetadata<Message> m) {
    String s = getMessage(m.message());
    String key = s.substring(0,s.indexOf("|"));
    String rest = s.substring(s.indexOf("|")+1);
    if ( this.outs.containsKey(key) ){
      try {
        this.outs.get(key).write(rest+"\n");
      } catch (IOException ex) {
        Logger.getLogger(CaligraphyMessageHandler.class.getName()).log(Level.SEVERE, null, ex);
      }
    } else {
      try {
        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter
                (fs.create(new Path("/tmp/events", key) )));
        this.outs.put(key, bw);
        this.outs.get(key).write(rest+"\n");
      } catch (IOException ex) {
        Logger.getLogger(CaligraphyMessageHandler.class.getName()).log(Level.SEVERE, null, ex);
      }
    }

  }

  
  @Override
  public void setWorkerThread(WorkerThread wt) {
    this.wt=wt;
    
    Configuration conf = new Configuration();
    try {
      //in 'real' life you would do something like this
      //conf.set("fs.default.name", w.properties.get("fs.default.name"));
      fs = FileSystem.get(conf);
    } catch (IOException ex) {
      Logger.getLogger(CaligraphyMessageHandler.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }

  @Override
  public void stop() {
    for (Map.Entry<String,Writer> entry:
      this.outs.entrySet() ){
      if (entry.getValue()!=null){
        try {
          entry.getValue().flush();
          entry.getValue().close();
        } catch (IOException ex) { }
      }
    }
  }
  
}

