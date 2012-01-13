package com.jointhegrid.ironcount;

import java.util.*;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

public class WorkerThread implements Runnable{
  enum WorkerThreadStatus { NEW,INIT,RUNNING,DONE };
  WorkerThreadStatus status;
  Workload workload;
  ConsumerConnector consumerConnector;
  ConsumerConfig config;
  MessageHandler handler;
  Properties props;
  boolean goOn;
  //UUID wtId;

  public WorkerThread(Workload w){
    status=WorkerThreadStatus.NEW;
    workload=w;
    goOn=true;
    //wtId = UUID.randomUUID();
  }

  @Override
  public void run(){
    status=WorkerThreadStatus.INIT;
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
    handler.setWorkerThread(this);

    Map<String,Integer> consumers = new HashMap<String,Integer>();
    consumers.put(workload.topic, 1);
    Map<String,List<KafkaMessageStream<Message>>> topicMessageStreams =
            consumerConnector.createMessageStreams(consumers);
    List<KafkaMessageStream<Message>> streams =
            topicMessageStreams.get(workload.topic);

    for (KafkaMessageStream<Message> stream:streams){
      ConsumerIterator<Message> it= stream.iterator();
      status=WorkerThreadStatus.RUNNING;
      while (goOn && it.hasNext() ){
        handler.handleMessage(it.next());

      }
    }
    System.err.println("thread end");
    status=WorkerThreadStatus.DONE;
  }
}

// offset storage ?
//stop ?
//faults ?
//maybe an enum for the state
