package com.jointhegrid.ironcount;

import java.util.HashMap;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataLayerTest {
  static EmbeddedCassandraService ecs;
  static Cluster cluster;
  static DataLayer d;

  @BeforeClass
  public static void setUpClass() throws Exception {
    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();
    ecs = new EmbeddedCassandraService();
    ecs.start();
    cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9157");
    d = new DataLayer(cluster);
    d.createMetaInfo();
  }

  @Test
  public void aTest(){
    
    Workload w = new Workload();
    w.active=true;
    w.consumerGroup="mygroup";
    w.maxWorkers=4;
    w.messageHandlerName="com.myorg.myhandler";
    w.name="myworkload";
    w.properties = new HashMap<String,String>();
    w.topic="stuff";


    Workload w1 = new Workload();
    w1.active=true;
    w1.consumerGroup="mygroup1";
    w1.maxWorkers=4;
    w1.messageHandlerName="com.myorg.myhandler1";
    w1.name="myworkload1";
    w1.properties = new HashMap<String,String>();
    w1.topic="stuff1";

    d.startWorkload(w);
    Assert.assertEquals(w ,d.getWorkloads().get(0));
    IronWorker iw = new IronWorker();
    d.registerJob(w, iw);

    IronWorker iw2 = new IronWorker();
    d.registerJob(w, iw2);

    JobInfo i = d.getJobInfoForWorkload(w);
    Assert.assertEquals(w.name, i.workloadName);
    Assert.assertEquals(true, i.workerIds.contains(iw.myId.toString()));
    Assert.assertEquals(true, i.workerIds.contains(iw2.myId.toString()));

  }
}
