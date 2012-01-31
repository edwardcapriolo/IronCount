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
import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;
import org.junit.Assert;
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
