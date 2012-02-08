/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jointhegrid.ironcount.caligraphy;

import com.jointhegrid.ironcount.IronIntegrationTest;
import com.jointhegrid.ironcount.StringSerializer;
import com.jointhegrid.ironcount.Workload;
import com.jointhegrid.ironcount.WorkloadManager;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.javaapi.producer.ProducerData;
import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;
import org.junit.Test;

/**
 *
 * @author edward
 */
public class CaligraphyIntegrationTest extends IronIntegrationTest {

   @Test
  public void mockingBirdDemo() {


    Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "com.jointhegrid.ironcount.caligraphy.CaligraphyMessageHandler";
    w.name = "cworkload";
    w.properties = new HashMap<String, String>();
    w.topic = topic;
    w.zkConnect = "localhost:8888";

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    Properties p = System.getProperties();
    p.put(WorkloadManager.ZK_SERVER_LIST, "localhost:8888");
    WorkloadManager m = new WorkloadManager(p);
    m.init();

    m.startWorkload(w);

    producer.send(new ProducerData<Integer, String>(topic, "2012-01-01|/index.htm|34.34.34.34"));
    producer.send(new ProducerData<Integer, String>(topic, "2012-01-01|/index.htm|34.34.34.34"));
    producer.send(new ProducerData<Integer, String>(topic, "2012-01-02|/index.htm|34.34.34.34"));

    try {
      Thread.sleep(10000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }


    m.shutdown();
  }

}
