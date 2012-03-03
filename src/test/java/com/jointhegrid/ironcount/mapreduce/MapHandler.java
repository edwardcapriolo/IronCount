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
    producerConfig = new ProducerConfig(producerProps);
    producer = new Producer<String,String>(producerConfig);
  }

  @Override
  public void handleMessage(Message m) {
    //message looks like this
    //users|1:edward
    //or
    //cart|1:saw
    String line = getMessage(m);
    String[] parts = line.split("\\|");
    String table = parts[0];
    String row = parts[1];
    String [] columns = row.split(":");

    //results look like this
    //Partitioner (1) users|1:edward
    //or
    //partitioner (1) cart|1:saw

    producer.send(new ProducerData<String, String>
            ("reduce", columns[0], Arrays.asList(table+"|"+row)));

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

  @Override
  public void stop() {
  }


}
