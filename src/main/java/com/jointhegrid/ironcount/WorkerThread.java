package com.jointhegrid.ironcount;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class WorkerThread implements Runnable, Watcher{

  @Override
  public void process(WatchedEvent we) {

  }
  
  enum WorkerThreadStatus { NEW,INIT,RUNNING,DONE };
  WorkerThreadStatus status;
  Workload workload;
  ConsumerConnector consumerConnector;
  ConsumerConfig config;
  MessageHandler handler;
  Properties props;
  boolean goOn;
  UUID wtId;
  ZooKeeper zk;

  public WorkerThread(WorkloadManager m, Workload w) {
    status=WorkerThreadStatus.NEW;
    workload=w;
    goOn=true;
    try {
      zk = new ZooKeeper(
              m.getProps().getProperty(WorkloadManager.ZK_SERVER_LIST), 3000, this);
     
      wtId = UUID.randomUUID();
    } catch (IOException ex) {
      Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
    }
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
    
    try {
      zk.create("/ironcount/workloads/" + this.workload.name + "/" + this.wtId,
              new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException ex) {
      Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
    } catch (InterruptedException ex) {
      Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
    }

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