package com.jointhegrid.ironcount;

import java.util.HashMap;
import java.util.Properties;
import kafka.javaapi.producer.ProducerData;
import org.junit.Assert;
import org.junit.Test;

public class HandlerExceptionIntegrationTest extends IronIntegrationTest {

  @Test
  public void disableWorkload() {
    Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "com.jointhegrid.ironcount.HeartAttachHandler";
    w.name = "testworkload";
    w.properties = new HashMap<String, String>();
    w.topic = topic;
    w.zkConnect = "localhost:8888";

    Properties p = new Properties();
    p.put(WorkloadManager.ZK_SERVER_LIST, "localhost:8888");
    HeartAttachHandler h = new HeartAttachHandler();

    WorkloadManager m = new WorkloadManager(p);
    m.init();

    WorkloadManager m2 = new WorkloadManager(p);
    m2.init();


    producer.send(new ProducerData<Integer, String>(topic, "1"));
    producer.send(new ProducerData<Integer, String>(topic, "2"));



    m.startWorkload(w);

    try {
      Thread.sleep(8000);
    } catch (InterruptedException ex) {
    }
    Assert.assertEquals(1, m.getWorkerThreads().size());
    Assert.assertEquals(1, m2.getWorkerThreads().size());
    producer.send(new ProducerData<Integer, String>(topic, "3"));

    producer.send(new ProducerData<Integer, String>(topic, "4"));
    producer.send(new ProducerData<Integer, String>(topic, "5"));
    producer.send(new ProducerData<Integer, String>(topic, "6"));
    producer.send(new ProducerData<Integer, String>(topic, "7"));
    producer.send(new ProducerData<Integer, String>(topic, "8"));
    producer.send(new ProducerData<Integer, String>(topic, "9"));

    producer.send(new ProducerData<Integer, String>(topic, "10"));
    producer.send(new ProducerData<Integer, String>(topic, "11"));
    try {
      Thread.sleep(12000);
    } catch (InterruptedException ex) {
    }

    Assert.assertEquals(11, h.messageCount.get());
  }
}
