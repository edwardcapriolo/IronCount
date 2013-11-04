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

import org.junit.Before;

import com.jointhegrid.ironcount.manager.WorkerThread;

public abstract class IronIntegrationTest extends BaseEmbededServerSetupTest {

  public static KafkaServer server;
  public Properties consumerProps;
  public Properties producerProps;
  public Properties brokerProps;

  public  KafkaConfig config;

  public static ConsumerConnector consumerConnector;
  public Producer producer;
  public ProducerConfig producerConfig;
  public ConsumerConfig consumerConfig;

  public static final String EVENTS = "events";

  public static EmbeddedZookeeper zk;

  public static final String LOCAL_ZK_ADDRESS = "localhost:8888";
  
  public static void createEventsTopic() {
    String[] arguments = new String[8];
    arguments[0] = "--zookeeper";
    arguments[1] = LOCAL_ZK_ADDRESS;
    arguments[2] = "--replica";
    arguments[3] = "1";
    arguments[4] = "--partition";
    arguments[5] = "1";
    arguments[6] = "--topic";
    arguments[7] = EVENTS;

    CreateTopicCommand.main(arguments);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @Before
  public void setupLocal() throws Exception{
    System.out.println("setup local");
    if ( zk == null ) {

      zk = new EmbeddedZookeeper(8888);
      zk.prepair();
      zk.start();
    }

    File ks1logdir = new File("/tmp/ks1logdir");
    ks1logdir.mkdir();

    brokerProps = new Properties();
    brokerProps.put("enable.zookeeper","true");
    brokerProps.put( "broker.id", "777");
    WorkerThread.putZkConnect(brokerProps, "localhost:8888");
    /*
    brokerProps.put("zk.connect", "localhost:8888");*/
    brokerProps.put("port","9092");

    consumerProps = new Properties();
    WorkerThread.putZkConnect(consumerProps, "localhost:8888");

    producerProps = new Properties();
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    WorkerThread.putZkConnect(producerProps, "localhost:8888");
    
    producerProps.put("metadata.broker.list", "localhost:9092");
    WorkerThread.putGroupId(consumerProps, "group1");
    
    consumerConfig = new ConsumerConfig(consumerProps);
    producerConfig = new ProducerConfig(producerProps);

    brokerProps.setProperty("topic.partition.count.map", EVENTS+":2"+",map:2,reduce:2");
    brokerProps.setProperty("num.partitions", "10");
    brokerProps.setProperty("log.dir", "/tmp/ks1logdir");

    File f = new File ("/tmp/ks1logdir");
    EmbeddedZookeeper.delete(f);

    config = new KafkaConfig(brokerProps);

    producer = new Producer<Integer,String>(producerConfig);

    
    if (server == null) {
      server = new kafka.server.KafkaServer(config, null);
      server.startup();
      consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }
    createEventsTopic();
  }

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
