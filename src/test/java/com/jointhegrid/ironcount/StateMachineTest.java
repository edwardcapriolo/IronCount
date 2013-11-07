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

import com.jointhegrid.ironcount.manager.Workload;
import com.jointhegrid.ironcount.manager.WorkloadManager;
import java.util.HashMap;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.junit.Assert;
import org.junit.Test;

public class StateMachineTest extends IronIntegrationTest {

  private static String EVENTS = "events";

  @Test
  public void disableWorkload() {
    createTopic(EVENTS, 1, 1);
    Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "com.jointhegrid.ironcount.SimpleMessageHandler";
    w.name = "testworkload";
    w.properties = new HashMap<String, String>();
    w.topic = EVENTS;
    w.zkConnect = super.zookeeperTestServer.getConnectString();

    Producer<String, String> producer = new Producer<String, String>(super.createProducerConfig());

    Properties p = new Properties();
    p.put(WorkloadManager.ZK_SERVER_LIST, super.zookeeperTestServer.getConnectString());
    SimpleMessageHandler sh = new SimpleMessageHandler();

    WorkloadManager m = new WorkloadManager(p);
    m.init();

    producer.send(new KeyedMessage<String, String>(EVENTS, "1"));
    producer.send(new KeyedMessage<String, String>(EVENTS, "2"));
    producer.send(new KeyedMessage<String, String>(EVENTS, "3"));

    m.applyWorkload(w);
    try {
      Thread.sleep(6000);
    } catch (InterruptedException ex) {
    }

    Assert.assertEquals(1, m.getWorkerThreads().size());
    Assert.assertEquals(3, sh.messageCount.get());

    w.active = false;
    m.applyWorkload(w);

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

    producer.send(new KeyedMessage<String, String>(EVENTS, "4"));
    producer.send(new KeyedMessage<String, String>(EVENTS, "5"));
    producer.send(new KeyedMessage<String, String>(EVENTS, "6"));

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
    }
    Assert.assertEquals(0, m.getWorkerThreads().size());

    w.active = true;
    m.applyWorkload(w);

    producer.send(new KeyedMessage<String, String>(EVENTS, "7"));
    producer.send(new KeyedMessage<String, String>(EVENTS, "8"));
    producer.send(new KeyedMessage<String, String>(EVENTS, "9"));

    try {
      Thread.sleep(3000);
    } catch (InterruptedException ex) {
    }

    Assert.assertEquals(9, sh.messageCount.get());

    m.shutdown();
  }
}
