/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jointhegrid.ironcount;

import java.util.HashMap;
import kafka.javaapi.producer.ProducerData;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author edward
 */
public class DisableWorkloadTest extends IronIntegrationTest {

  @Test
  public void disableWorkload(){
    Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "com.jointhegrid.ironcount.SimpleMessageHandler";
    w.name = "testworkload";
    w.properties = new HashMap<String, String>();
    w.topic = topic;
    w.zkConnect = "localhost:8888";

    SimpleMessageHandler sh = new SimpleMessageHandler();
    IroncountWorkloadManager m = new IroncountWorkloadManager(this.cluster);
    m.init();
    m.execute();

    dl.startWorkload(w);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException ex) {
    }
   

    producer.send(new ProducerData<Integer, String>(topic, "1"));
    producer.send(new ProducerData<Integer, String>(topic, "2"));
    producer.send(new ProducerData<Integer, String>(topic, "3"));

    try {
      Thread.sleep(9000);
    } catch (InterruptedException ex) {
    }

    dl.stopWorkload(w);

     try {
      Thread.sleep(3000);
    } catch (InterruptedException ex) {
    }

    producer.send(new ProducerData<Integer, String>(topic, "4"));
    producer.send(new ProducerData<Integer, String>(topic, "5"));


    Assert.assertEquals(3,sh.messageCount.get());
    m.shutdown();
  }
}
