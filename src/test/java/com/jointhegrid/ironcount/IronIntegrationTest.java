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
package com.jointhegrid.ironcount;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

import kafka.admin.CreateTopicCommand;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.jointhegrid.ironcount.manager.WorkerThread;
import com.netflix.curator.test.TestingServer;

public abstract class IronIntegrationTest extends BaseEmbededServerSetupTest {

  public static final String EVENTS = "events";
  public static KafkaServer server;
  public static ConsumerConnector consumerConnector;
  public Producer producer;
  public static TestingServer zookeeperTestServer ;


  public static void createTopic(String name, int replica, int partitions ) {
    String[] arguments = new String[8];
    arguments[0] = "--zookeeper";
    arguments[1] = "localhost:"+zookeeperTestServer.getPort();
    arguments[2] = "--replica";
    arguments[3] = replica+"";
    arguments[4] = "--partition";
    arguments[5] = partitions+"";
    arguments[6] = "--topic";
    arguments[7] = name;

    CreateTopicCommand.main(arguments);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception{
    String kdir = "/tmp/ks1logdir";
    zookeeperTestServer = new TestingServer();
    
    File ks1logdir = new File(kdir);
    if (ks1logdir.exists()){
      EmbeddedZookeeper.delete(ks1logdir);
    }
    ks1logdir.mkdir();
    Properties brokerProps= new Properties();
    brokerProps.put("enable.zookeeper","true");
    brokerProps.put( "broker.id", "1");
    WorkerThread.putZkConnect(brokerProps, "localhost:"+zookeeperTestServer.getPort());
    brokerProps.put("port","9092");
    brokerProps.setProperty("topic.partition.count.map", EVENTS+":1"+",map:2,reduce:2");
    brokerProps.setProperty("num.partitions", "10");
    brokerProps.setProperty("log.dir", "/tmp/ks1logdir");
    KafkaConfig config= new KafkaConfig(brokerProps);
    if (server == null) {
      server = new kafka.server.KafkaServer(config, new Realtime());
      server.startup();
    }
    
  }
  
  protected static ProducerConfig createProducerConfig(){
    Properties producerProps = new Properties();
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    WorkerThread.putZkConnect(producerProps, "localhost:"+zookeeperTestServer.getPort());
    producerProps.setProperty("batch.size", "10");
    producerProps.setProperty("producer.type", "async");
    producerProps.put("metadata.broker.list", "localhost:9092");
    return new ProducerConfig(producerProps); 
  }

  protected ConsumerConfig createConsumerConfig(){
    Properties consumerProps = new Properties();
    WorkerThread.putZkConnect(consumerProps, "localhost:"+zookeeperTestServer.getPort());
    WorkerThread.putGroupId(consumerProps, "group1");
    consumerProps.put("auto.offset.reset", "smallest");
    ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
    return consumerConfig;
  }
  
  @After
  public void cleanup(){
    
  }

}
class Realtime implements Time { 

  public Realtime(){
    
  }

  @Override
  public long milliseconds() {
    return System.currentTimeMillis();
  }

  @Override
  public long nanoseconds() {
    return System.nanoTime();
  }

  @Override
  public void sleep(long arg0) {
    try {
    Thread.sleep(arg0);
    } catch (Exception ex){}
  }
  
};
