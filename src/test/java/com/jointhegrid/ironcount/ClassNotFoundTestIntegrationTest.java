package com.jointhegrid.ironcount;

import com.jointhegrid.ironcount.manager.Workload;
import com.jointhegrid.ironcount.manager.WorkloadManager;
import java.util.HashMap;
import java.util.Properties;

import kafka.javaapi.producer.Producer;

import org.junit.Assert;
import org.junit.Test;

public class ClassNotFoundTestIntegrationTest
        extends IronIntegrationTest {

  private static String EVENTS = "events";
  @Test
  public void disableWorkload(){
    createTopic(EVENTS, 1, 1);
    
    Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "bad.class.name";
    w.name = "testworkload";
    w.properties = new HashMap<String, String>();
    w.topic = EVENTS;
    w.zkConnect = this.zookeeperTestServer.getConnectString();

    Properties p = new Properties();
    p.put(WorkloadManager.ZK_SERVER_LIST, this.zookeeperTestServer.getConnectString());
    WorkloadManager m = new WorkloadManager(p);
    m.init();

    m.applyWorkload(w);
    try {
      Thread.sleep(2000);
    } catch (InterruptedException ex) {
    }
    Assert.assertEquals(0, m.getWorkerThreads().size() );
    m.shutdown();
  }
}
