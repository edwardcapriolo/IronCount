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
package com.jointhegrid.ironcount.manager;

import com.jointhegrid.ironcount.classloader.ICURLClassLoader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class WorkerThread implements Runnable, Watcher, WorkerThreadMBean {

  final static Logger logger = Logger.getLogger(WorkerThread.class.getName());
  public static final String MBEAN_OBJECT_NAME = "com.jointhegrid.ironcount:type=WorkerThread";

  private final AtomicLong messagesProcessesed ;
  private final AtomicLong processingTime;
  enum WorkerThreadStatus { NEW,INIT,RUNNING,DONE };
  WorkerThreadStatus status;
  Workload workload;
  public ConsumerConnector consumerConnector;
  public ConsumerConfig config;
  public MessageHandler handler;
  public Properties props;
  boolean goOn;
  private WorkloadManager m;
  UUID wtId;
  ZooKeeper zk;
  ExecutorService executor;

  public WorkerThread(WorkloadManager m, Workload w) {
    messagesProcessesed = new AtomicLong(0);
    processingTime = new AtomicLong(0);
    status=WorkerThreadStatus.NEW;
    wtId = UUID.randomUUID();
    workload=w;
    goOn=true;
    this.m=m;
    executor = Executors.newFixedThreadPool(1);
    try {
      zk = new ZooKeeper(
              m.getProps().getProperty(WorkloadManager.ZK_SERVER_LIST), 3000, this);
    } catch (IOException ex) {
      logger.error(ex);
      throw new RuntimeException(ex);
    }

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    try {
      mbs.registerMBean(this, new ObjectName(MBEAN_OBJECT_NAME+",uuid="+wtId));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

   @Override
  public void process(WatchedEvent we) {
    logger.debug(we);
    if (we.getType()== EventType.NodeDataChanged){
     if (we.getPath().equals("/ironcount/workloads/"+ this.workload.name)){
        logger.debug("change detected "+we);
        try {
          Stat s = zk.exists("/ironcount/workloads/" + this.workload.name, false);
          byte [] dat = zk.getData("/ironcount/workloads/"+ this.workload.name, false, s);
          Workload w = this.m.deserializeWorkload(dat);
          if (w.active.equals(Boolean.FALSE)){
            this.goOn=false;
            this.executor.shutdown();
            logger.debug("Shutdown");
          }
        } catch (KeeperException ex) {
          logger.error(ex);
          throw new RuntimeException (ex);
        } catch (InterruptedException ex) {
          logger.error(ex);
          throw new RuntimeException (ex);
        }
      }
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
      handler = (MessageHandler) new ICURLClassLoader().getClassLoader(workload)
              .loadClass(this.workload.messageHandlerName).newInstance();
    } catch (Exception ex) {
      logger.error(ex);
      this.terminate();
    }

    handler.setWorkload(this.workload);
    handler.setWorkerThread(this);
    
    try {
      zk.create("/ironcount/workloads/" + this.workload.name + "/" + this.wtId,
              new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      zk.exists("/ironcount/workloads/"+ this.workload.name , this);
    } catch (KeeperException ex) {
      logger.error(ex);
      this.terminate();
    } catch (InterruptedException ex) {
      logger.error(ex);
      this.terminate();
    }

    Map<String,Integer> consumers = new HashMap<String,Integer>();
    consumers.put(workload.topic, 1);
    Map<String,List<KafkaMessageStream<Message>>> topicMessageStreams =
            consumerConnector.createMessageStreams(consumers);
    List<KafkaMessageStream<Message>> streams =
            topicMessageStreams.get(workload.topic);

    for(final KafkaMessageStream<Message> stream: streams) {
      executor.submit(new Runnable() {
        public void run() {
          for(Message message: stream) {
            try {
              if (goOn){
                long before=System.nanoTime();
                handler.handleMessage(message);
                long after=System.nanoTime();
                processingTime.addAndGet(after-before);
                messagesProcessesed.addAndGet(1);
              }
            } catch (Exception ex){
              logger.error("worker thread fired exception "+workload+" "+ex);
              goOn=false;
              terminate();
            }
          }
        }
      });
    }

    while(goOn){
      try {
        Thread.sleep(1);
      } catch (InterruptedException ex) {
        logger.error(ex);
        this.terminate();
      }
    }

    logger.debug("WorkerThread end");
    terminate();
    
  }

  @Override
  public void terminate(){
    try {
      this.goOn=false;
      this.m.getWorkerThreads().remove(this);
      this.zk.close();
      this.handler.stop();
      executor.shutdown();
      logger.debug("WorkerThread tear down");
      status=WorkerThreadStatus.DONE;
      if (consumerConnector !=null){
        consumerConnector.shutdown();
      }
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    try {
      mbs.unregisterMBean(new ObjectName(MBEAN_OBJECT_NAME+",uuid="+wtId));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
   
    } catch (InterruptedException ex) {
      logger.warn(ex);
    }
  }
  
  public UUID getWtId() {
    return wtId;
  }

  public void setWtId(UUID wtId) {
    this.wtId = wtId;
  }

  @Override
  public String getJSONSerializedWorkload() {
    byte [] b = m.serializeWorkload(this.workload);
    return new String(b);
  }

  @Override
  public long getMessagesProcessesed() {
    return messagesProcessesed.get();
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
  }

  @Override
  public long getProcessingTime() {
    return processingTime.get();
  }

}