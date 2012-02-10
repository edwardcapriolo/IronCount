package com.jointhegrid.ironcount;

import java.util.HashMap;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

public class ClassNotFoundTestIntegrationTest
        extends IronIntegrationTest {

  @Test
  public void disableWorkload(){
    Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "bad.class.name";
    w.name = "testworkload";
    w.properties = new HashMap<String, String>();
    w.topic = topic;
    w.zkConnect = "localhost:8888";

    Properties p = new Properties();
    p.put(WorkloadManager.ZK_SERVER_LIST, "localhost:8888");
    WorkloadManager m = new WorkloadManager(p);
    m.init();

    m.startWorkload(w);
    try {
      Thread.sleep(2000);
    } catch (InterruptedException ex) {
    }
    Assert.assertEquals(0, m.getWorkerThreads().size() );
    m.shutdown();
  }
}
