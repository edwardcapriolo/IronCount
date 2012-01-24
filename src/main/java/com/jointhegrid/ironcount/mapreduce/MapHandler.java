package com.jointhegrid.ironcount.mapreduce;

import com.jointhegrid.ironcount.MessageHandler;
import com.jointhegrid.ironcount.WorkerThread;
import com.jointhegrid.ironcount.Workload;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;

public class MapHandler implements MessageHandler {

  private Workload w;
  private WorkerThread wt;

  private ProducerConfig producerConfig;
  private Properties producerProps;
  private Producer producer;
  
  public MapHandler(){
    
  }

  @Override
  public void setWorkload(Workload w) {
    this.w=w;

    producerProps = new Properties();
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    producerProps.put("zk.connect", w.properties.get("zk.connect"));

    producer = new Producer<Integer,String>(producerConfig);
  }

  @Override
  public void handleMessage(Message m) {
    String line = getMessage(m);
    String[] parts = line.split("|");
    String key = parts[0];
    String value = parts[1];
   
    producer.send(new ProducerData<String, String>
            ("reduce", key, Arrays.asList(key+"|"+1)));
  }

  @Override
  public void setWorkerThread(WorkerThread wt) {
    this.wt=wt;
  }

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
