package com.jointhegrid.ironcount;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class WorkerThread implements Runnable, Watcher{

  @Override
  public void process(WatchedEvent we) {
    System.out.println("Worker Thread----------------------"+we);
    if (we.getType()== EventType.NodeDataChanged){
     if (we.getPath().equals("/ironcount/workloads/"+ this.workload.name)){
        System.out.println("my workload changed----------------------");
        try {
          //reconsitute workload
          Stat s = zk.exists("/ironcount/workloads/" + this.workload.name, false);
          byte [] dat = zk.getData("/ironcount/workloads/"+ this.workload.name, false, s);
          Workload w = this.m.deserializeWorkload(dat);
          if (w.active.equals(Boolean.FALSE)){
            this.goOn=false;
            this.executor.shutdown();
            System.out.println("shuting myself down");
          }
        } catch (KeeperException ex) {
          Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
          Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    }
  }
  
  enum WorkerThreadStatus { NEW,INIT,RUNNING,DONE };
  WorkerThreadStatus status;
  Workload workload;
  ConsumerConnector consumerConnector;
  ConsumerConfig config;
  MessageHandler handler;
  Properties props;
  boolean goOn;
  private WorkloadManager m;
  UUID wtId;
  ZooKeeper zk;
  ExecutorService executor;

  public WorkerThread(WorkloadManager m, Workload w) {
    status=WorkerThreadStatus.NEW;
    workload=w;
    goOn=true;
    this.m=m;
    executor = Executors.newFixedThreadPool(1);
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
      throw new RuntimeException(ex);
    }

    handler.setWorkload(this.workload);
    handler.setWorkerThread(this);
    
    try {
      zk.create("/ironcount/workloads/" + this.workload.name + "/" + this.wtId,
              new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      zk.exists("/ironcount/workloads/"+ this.workload.name , this);
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    Map<String,Integer> consumers = new HashMap<String,Integer>();
    consumers.put(workload.topic, 1);
    Map<String,List<KafkaMessageStream<Message>>> topicMessageStreams =
            consumerConnector.createMessageStreams(consumers);
    List<KafkaMessageStream<Message>> streams =
            topicMessageStreams.get(workload.topic);

    

    /*
    for (KafkaMessageStream<Message> stream:streams){
      ConsumerIterator<Message> it= stream.iterator();
      status=WorkerThreadStatus.RUNNING;
      while (goOn && it.hasNext() ){
        handler.handleMessage(it.next());

      }
    }
     *
     */
    for(final KafkaMessageStream<Message> stream: streams) {
      executor.submit(new Runnable() {
        public void run() {
          for(Message message: stream) {
            handler.handleMessage(message);
          }
        }
      });
    }

    while(goOn){
      try {
        Thread.sleep(1);
      } catch (InterruptedException ex) {
        Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
      }
    }

    System.err.println("thread end");
    try {
      this.m.getWorkerThreads().remove(this);
      this.zk.close();
    } catch (InterruptedException ex) {
      Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
    }
    status=WorkerThreadStatus.DONE;
  }

}