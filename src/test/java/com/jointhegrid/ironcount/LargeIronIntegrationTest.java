/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jointhegrid.ironcount;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.javaapi.producer.ProducerData;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author edward
 */
public class LargeIronIntegrationTest extends IronIntegrationTest {

  public LargeIronIntegrationTest() throws Exception{
  }

  @BeforeClass
  public static void setUpClass() throws Exception {

  }

  @AfterClass
  public static void tearDownClass() throws Exception {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /*
   @Test
  public void hello() {

    Workload w = new Workload();
    w.active=true;
    w.consumerGroup="group1";
    w.maxWorkers=4;
    w.messageHandlerName="com.jointhegrid.ironcount.SimpleMessageHandler";
    w.name="testworkload";
    w.properties=new HashMap<String,String>();
    w.topic=topic;
    w.zkConnect="localhost:8888";

    //DataLayer dl = new DataLayer(this.cluster);
    //dl.createMetaInfo();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    IronWorker iw = new IronWorker();
    iw.runDaemon();

    IronWorker iw2 = new IronWorker();
    iw2.runDaemon();

    dl.startWorkload(w);

    for (int i =0;i<2000;i++){
      producer.send(new ProducerData<Integer, String>(topic, i+" 1 b c"));
      producer.send(new ProducerData<Integer, String>(topic, i+" d e f"));
     }
    try {
      Thread.sleep(8000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    SimpleMessageHandler h = new SimpleMessageHandler();
    Assert.assertEquals(4000, h.messageCount.get());
    //Assert.assertEquals(true, h.handlerCount.get()>=2);

    iw.stop();
  }
  */
}
