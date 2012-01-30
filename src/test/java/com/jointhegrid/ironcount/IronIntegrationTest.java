package com.jointhegrid.ironcount;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.junit.Before;

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

  public String topic="events";

  public static EmbeddedZookeeper zk;

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

    brokerProps.setProperty("topic.partition.count.map", topic+":2"+",map:2,reduce:2");
    brokerProps.setProperty("num.partitions", "10");
    brokerProps.setProperty("brokerid", "1");
    brokerProps.setProperty("log.dir", "/tmp/ks1logdir");

    File f = new File ("/tmp/ks1logdir");
    EmbeddedZookeeper.delete(f);

    config = new KafkaConfig(brokerProps);

    producer = new Producer<Integer,String>(producerConfig);

    if ( server == null ) {
      server = new kafka.server.KafkaServer(config);
      server.startup();
      consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }
  }

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
