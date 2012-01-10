package com.jointhegrid.ironcount;

import java.util.*;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

public class WorkerThread implements Runnable{
  Workload workload;
  ConsumerConnector consumerConnector;
  ConsumerConfig config;
  MessageHandler handler;
  Properties props;
  boolean goOn;
  //UUID wtId;

  public WorkerThread(Workload w){
    workload=w;
    goOn=true;
    //wtId = UUID.randomUUID();
  }

  @Override
  public void run(){
    
    props = new Properties();
    props.put("groupid", workload.consumerGroup);
    props.put("zk.connect", workload.zkConnect);
    config = new ConsumerConfig(props);
    consumerConnector = Consumer.createJavaConsumerConnector(config);

    try {
      handler = (MessageHandler) Class.forName(this.workload.messageHandlerName).newInstance();
    } catch (Exception ex) {
      System.err.println(ex.toString());
    }
    handler.setWorkload(this.workload);

    Map<String,Integer> consumers = new HashMap<String,Integer>();
    consumers.put(workload.topic, 1);
    Map<String,List<KafkaMessageStream<Message>>> topicMessageStreams =
            consumerConnector.createMessageStreams(consumers);
    List<KafkaMessageStream<Message>> streams =
            topicMessageStreams.get(workload.topic);
    for (KafkaMessageStream<Message> stream:streams){
      //for(Message message:stream){
      //  handler.handleMessage(message);
      //}
      ConsumerIterator<Message> it= stream.iterator();
      while (it.hasNext() && goOn){
        handler.handleMessage(it.next());
      }
    }
    System.err.println("thread end");
  }
}

// offset storage ?
//stop ?
//faults ?
