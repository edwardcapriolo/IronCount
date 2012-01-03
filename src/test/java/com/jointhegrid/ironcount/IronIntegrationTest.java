package com.jointhegrid.ironcount;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;


import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IronIntegrationTest {

  static EmbeddedCassandraService ecs;
  static Cluster cluster;

  static KafkaServer server;
  static Properties consumerProps;
  static Properties producerProps;
  static Properties brokerProps;

  static KafkaConfig config;

  static ConsumerConnector consumerConnector;
  static Producer producer;
  static ProducerConfig producerConfig;
  static ConsumerConfig consumerConfig;

  static String topic="events";

  static EmbeddedZookeeper zk;

  public IronIntegrationTest() {
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();
    ecs = new EmbeddedCassandraService();
    ecs.start();
    cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9157");

    zk = new EmbeddedZookeeper(8888);
    zk.prepair();
    zk.start();

    File ks1logdir = new File("/tmp/ks1logdir");
    ks1logdir.mkdir();

    brokerProps = new Properties();
    brokerProps.put("enable.zookeeper","true");
    brokerProps.put("zk.connect", "localhost:8888");
    brokerProps.put("port","9092");

    consumerProps = new Properties();
    consumerProps.put("zk.connect", "localhost:8888");

    producerProps = new Properties();
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    producerProps.put("zk.connect", "localhost:8888");

    consumerProps.setProperty("groupid", "group1");
    consumerConfig = new ConsumerConfig(consumerProps);
    producerConfig = new ProducerConfig(producerProps);

    brokerProps.setProperty("topic.partition.count.map", topic+":2");
    brokerProps.setProperty("num.partitions", "2");
    brokerProps.setProperty("brokerid", "1");
    brokerProps.setProperty("log.dir", "/tmp/ks1logdir");

    File f = new File ("/tmp/ks1logdir");
    EmbeddedZookeeper.delete(f);

    config = new KafkaConfig(brokerProps);

    producer = new Producer<Integer,String>(producerConfig);

    server = new kafka.server.KafkaServer(config);
    server.startup();
    consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (server !=null){
      server.shutdown();
    }
  }

  @Test
  public void hello() {

    Workload w = new Workload();
    w.active=true;
    w.consumerGroup="group1";
    w.maxWorkers=1;
    w.messageHandlerName="com.jointhegrid.ironcount.SimpleMessageHandler";
    w.name="testworkload";
    w.properties=new HashMap<String,String>();
    w.topic=topic;
    w.zkConnect="localhost:8888";

    DataLayer dl = new DataLayer(this.cluster);
    dl.createMetaInfo();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }
    
    IronWorker iw = new IronWorker();
    iw.runDaemon();

    IronWorker iw2 = new IronWorker();
    iw2.runDaemon();

    dl.startWorkload(w);
    
    producer.send(new ProducerData<Integer, String>(topic, "1 b c"));
    producer.send(new ProducerData<Integer, String>(topic, "d e f"));
    try {
      Thread.sleep(3000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    SimpleMessageHandler h = new SimpleMessageHandler();
    Assert.assertEquals(2, h.messageCount);

    iw.stop();
  }

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
