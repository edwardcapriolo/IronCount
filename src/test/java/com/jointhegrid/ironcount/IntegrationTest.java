package com.jointhegrid.ironcount;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IntegrationTest {

   EmbeddedCassandraService ecs;
   Cluster cluster;

   KafkaServer server;
   Properties consumerProps;
   Properties producerProps;
   Properties brokerProps;
  
   KafkaConfig config;

   ConsumerConnector consumerConnector;
   Producer producer;
   ProducerConfig producerConfig;
   ConsumerConfig consumerConfig;

   String topic="events";

   EmbeddedZookeeper zk;

  public IntegrationTest() throws Exception{
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

    brokerProps.setProperty("topic.partition.count.map", "events:2");
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

  @BeforeClass
  public static void setUpClass() throws Exception {
  
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    //server.shutdown();
  }

  @Test
  public void hello() {
    producer.send(new ProducerData<Integer, String>(topic, "1 b c"));
    producer.send(new ProducerData<Integer, String>(topic, "d e f"));

    Map<String, Integer> consumers = new HashMap<String, Integer>();
    consumers.put(this.topic, 1);
    Map<String, List<KafkaMessageStream<Message>>> topicMessageStreams =
            consumerConnector.createMessageStreams(consumers);
    List<KafkaMessageStream<Message>> streams = topicMessageStreams.get(this.topic);

    int x=0;
    // consume the messages in the threads
    for (KafkaMessageStream<Message> stream : streams) {
      for (Message message : stream) {
        System.out.println(getMessage(message));
        x++;
        if (x==2){
          break;
        }
      }
    }
    
  }

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
