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

import java.util.HashMap;
import java.util.Properties;
import kafka.javaapi.producer.ProducerData;
import org.junit.Assert;
import org.junit.Test;

public class StateMachineTest extends IronIntegrationTest {

  @Test
  public void disableWorkload() {
    Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "com.jointhegrid.ironcount.SimpleMessageHandler";
    w.name = "testworkload";
    w.properties = new HashMap<String, String>();
    w.topic = topic;
    w.zkConnect = "localhost:8888";

    Properties p = new Properties();
    p.put(WorkloadManager.ZK_SERVER_LIST, "localhost:8888");
    SimpleMessageHandler sh = new SimpleMessageHandler();

    WorkloadManager m = new WorkloadManager(p);
    m.init();

    producer.send(new ProducerData<Integer, String>(topic, "1"));
    producer.send(new ProducerData<Integer, String>(topic, "2"));
    producer.send(new ProducerData<Integer, String>(topic, "3"));

    m.startWorkload(w);
    try {
      Thread.sleep(6000);
    } catch (InterruptedException ex) {
    }

    Assert.assertEquals(1, m.getWorkerThreads().size());
    Assert.assertEquals(3,sh.messageCount.get());


    w.active = false;
    m.stopWorkload(w);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException ex) {
    }
    Assert.assertEquals(0, m.getWorkerThreads().size());

    m.deleteWorkload(w);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException ex) {
    }


    producer.send(new ProducerData<Integer, String>(topic, "4"));
    producer.send(new ProducerData<Integer, String>(topic, "5"));
    producer.send(new ProducerData<Integer, String>(topic, "6"));

   

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) { }
    Assert.assertEquals(0, m.getWorkerThreads().size());

    w.active=true;
    m.startWorkload(w);

    producer.send(new ProducerData<Integer, String>(topic, "7"));
    producer.send(new ProducerData<Integer, String>(topic, "8"));
    producer.send(new ProducerData<Integer, String>(topic, "9"));

    try {
      Thread.sleep(3000);
    } catch (InterruptedException ex) { }

    Assert.assertEquals(9,sh.messageCount.get());

    m.shutdown();
  }
}
