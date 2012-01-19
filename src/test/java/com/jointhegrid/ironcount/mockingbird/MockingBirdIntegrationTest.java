 package com.jointhegrid.ironcount.mockingbird;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.jointhegrid.ironcount.IronIntegrationTest;
import com.jointhegrid.ironcount.WorkloadManager;
import com.jointhegrid.ironcount.Workload;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import kafka.javaapi.producer.ProducerData;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.model.thrift.ThriftColumnQuery;
import me.prettyprint.cassandra.model.thrift.ThriftCountQuery;
import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MockingBirdIntegrationTest extends IronIntegrationTest {

  
  @Test
  public void mockingBirdDemo() {

    KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition("mockingbird");
    cluster.addKeyspace(ksDef, true);
    Keyspace moch = HFactory.createKeyspace("mockingbird", cluster);
    CqlQuery<String, String, String> cqlQuery = new CqlQuery<String, String, String>(moch, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    cqlQuery.setQuery("create columnfamily mockingbird (key 'org.apache.cassandra.db.marshal.UTF8Type' primary key) with  "
            + "comparator = 'org.apache.cassandra.db.marshal.UTF8Type' "
            + "and default_validation = 'CounterColumnType' ");
    QueryResult<CqlRows<String, String, String>> result = cqlQuery.execute();

    Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "com.jointhegrid.ironcount.mockingbird.MockingBirdMessageHandler";
    w.name = "testworkload";
    w.properties = new HashMap<String, String>();
    w.topic = topic;
    w.zkConnect = "localhost:8888";

    //pass the cassandra information to the IronWorker
    w.properties.put("mocking.ks", "mockingbird");
    w.properties.put("mocking.cf", "mockingbird");
    w.properties.put("mocking.cas", "localhost:9157");

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
    
    producer.send(new ProducerData<Integer, String>(topic, "http://www.ed.com/stuff"));
    producer.send(new ProducerData<Integer, String>(topic, "http://toys.ed.com/toys"));
    producer.send(new ProducerData<Integer, String>(topic, "http://www.ed.com/"));

    try {
      Thread.sleep(5000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }


    /*
    ColumnFamilyTemplate<String,String> template =
            new ThriftColumnFamilyTemplate<String,String>(moch,"mockingbird"
            ,StringSerializer.get(),StringSerializer.get());
    ColumnFamilyResult<String,String> res =template.queryColumns
            ("com/"+  new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date()));
     *
     */

    CounterQuery <String, String> mcq = new ThriftCounterColumnQuery<String,String>
            (moch,StringSerializer.get(),StringSerializer.get());

    mcq.setKey("com/"+  new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date()));
    mcq.setColumnFamily("mockingbird");
    mcq.setName("count");

    QueryResult<HCounterColumn<String>> res = mcq.execute();
    Assert.assertEquals(new Long(3), res.get().getValue());

    mcq = new ThriftCounterColumnQuery<String,String>
            (moch,StringSerializer.get(),StringSerializer.get());

    mcq.setKey("com:ed:www/"+  new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date()));
    mcq.setColumnFamily("mockingbird");
    mcq.setName("count");

    res = mcq.execute();
    Assert.assertEquals(new Long(2), res.get().getValue());

    m.shutdown();
  }
}
