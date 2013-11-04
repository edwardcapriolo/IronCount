package com.jointhegrid.ironcount;

import com.jointhegrid.ironcount.manager.Workload;
import com.jointhegrid.ironcount.manager.WorkloadManager;
import java.util.HashMap;
import java.util.Properties;

import kafka.producer.KeyedMessage;

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
    w.topic = EVENTS;
    w.zkConnect = "localhost:8888";

    Properties p = new Properties();
    p.put(WorkloadManager.ZK_SERVER_LIST, "localhost:8888");
    HeartAttachHandler h = new HeartAttachHandler();

    WorkloadManager m = new WorkloadManager(p);
    m.init();

    WorkloadManager m2 = new WorkloadManager(p);
    m2.init();


    producer.send(new KeyedMessage<Integer, String>(EVENTS, "1"));
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "2"));



    m.applyWorkload(w);

    try {
      Thread.sleep(8000);
    } catch (InterruptedException ex) {
    }
    Assert.assertEquals(1, m.getWorkerThreads().size());
    Assert.assertEquals(1, m2.getWorkerThreads().size());
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "3"));

    producer.send(new KeyedMessage<Integer, String>(EVENTS, "4"));
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "5"));
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "6"));
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "7"));
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "8"));
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "9"));

    producer.send(new KeyedMessage<Integer, String>(EVENTS, "10"));
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "11"));
    try {
      Thread.sleep(12000);
    } catch (InterruptedException ex) {
    }

    Assert.assertEquals(11, h.messageCount.get());
  }
}
