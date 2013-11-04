package com.jointhegrid.ironcount;

import com.jointhegrid.ironcount.httpserver.ICHTTPServer;
import com.jointhegrid.ironcount.manager.Workload;
import com.jointhegrid.ironcount.manager.WorkloadManager;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import kafka.producer.KeyedMessage;

import org.junit.Test;

public class HandlerFromURLIntegrationTest extends IronIntegrationTest {

  @Test
  public void test() throws MalformedURLException, Exception{
     Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "com.jointhegrid.fromurl.MessageToFileHandlerFromURL";
    w.name = "fromURL";
    w.properties = new HashMap<String, String>();
    w.topic = EVENTS;
    w.zkConnect = "localhost:8888";
    w.classloaderUrls = new ArrayList<URL>();
    w.classloaderUrls.add( new URL("http://localhost:8766/") );

    ICHTTPServer serv = new ICHTTPServer();
    serv.docBase="/home/edward/ironcount/src/test/resources/urlload";
    serv.startServer();

    Properties p = System.getProperties();
    p.put(WorkloadManager.ZK_SERVER_LIST, "localhost:8888");
    WorkloadManager m = new WorkloadManager(p);
    m.init();

    m.applyWorkload(w);
    try {
      Thread.sleep(2000);
    } catch (InterruptedException ex) {}

    for (int i =0;i<1000;i++){
      producer.send(new KeyedMessage<Integer, String>(EVENTS,""+i));

    }

     try {
      Thread.sleep(5000);
    } catch (InterruptedException ex) {

    }

    w.active=false;
    m.applyWorkload(w);

    try {
      Thread.sleep(4000);
    } catch (InterruptedException ex) {
      
    }

    m.shutdown();


  }
}
