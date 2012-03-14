package com.jointhegrid.ironcount.caligraphy;

import com.jointhegrid.ironcount.IronIntegrationTest;
import com.jointhegrid.ironcount.manager.StringSerializer;
import com.jointhegrid.ironcount.manager.Workload;
import com.jointhegrid.ironcount.manager.WorkloadManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author edward
 */
public class CaligraphyIntegrationTest extends IronIntegrationTest {

   @Test
  public void mockingBirdDemo() throws FileNotFoundException, IOException {


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
    producer.send(new ProducerData<Integer, String>(topic, "2012-01-01|/index.htm|34.34.34.35"));
    producer.send(new ProducerData<Integer, String>(topic, "2012-01-02|/index.htm|34.34.34.36"));

    try {
      Thread.sleep(10000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    w.active=false;
    m.stopWorkload(w);
     try {
      Thread.sleep(2000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    //m.deleteWorkload(w);
    m.shutdown();

    FileReader fr = new FileReader("/tmp/events/2012-01-01");
    BufferedReader br = new BufferedReader(fr);
    String line1 = br.readLine();
    String line2 = br.readLine();
    br.close();
    Assert.assertEquals("/index.htm|34.34.34.34", line1);
    Assert.assertEquals("/index.htm|34.34.34.35", line2);
  }

}
