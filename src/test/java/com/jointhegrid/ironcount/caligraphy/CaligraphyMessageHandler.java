package com.jointhegrid.ironcount.caligraphy;

import com.jointhegrid.ironcount.MessageHandler;
import com.jointhegrid.ironcount.WorkerThread;
import com.jointhegrid.ironcount.Workload;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.message.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class CaligraphyMessageHandler implements MessageHandler {

  private Workload w;
  private WorkerThread wt;
  private Map<Date,DataCollector> outs;
  FileSystem fs;
  DateFormat df;

  public CaligraphyMessageHandler(){
    outs=new HashMap<Date,DataCollector>();
  }

  @Override
  public void setWorkload(Workload w) {
    this.w=w;
  }

  @Override
  public void handleMessage(Message m) {
    //field1 YYYY_MM_DD_HH_MM;
    String [] parts = getMessage(m).split("\\|");
    df = new SimpleDateFormat("yyyy-MM-dd");
    Date d = null;
    try {
      d = df.parse(parts[0]);
      write(d, parts);
    } catch (ParseException ex) {
      Logger.getLogger(CaligraphyMessageHandler.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void write(Date date, String [] parts){
    if ( outs.get(date) == null){
      outs.put(date, new DataCollector(this));
    } else {
      DataCollector dc = outs.get(date);
      dc.write(date, parts);
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
}

