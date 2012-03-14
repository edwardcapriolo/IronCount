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
package com.jointhegrid.ironcount.mapreduce;

import com.jointhegrid.ironcount.IronIntegrationTest;
import com.jointhegrid.ironcount.manager.Workload;
import com.jointhegrid.ironcount.manager.WorkloadManager;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Assert;
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
import org.junit.Test;

/**
 *
 * @author edward
 */
public class MapReduceJoinIntegrationTest extends IronIntegrationTest {

  @Test
  public void mapReduceJoin() {

     KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition("mr");
    cluster.addKeyspace(ksDef, true);
    Keyspace mr = HFactory.createKeyspace("mr", cluster);
    CqlQuery<String, String, String> cqlQuery = new CqlQuery<String, String, String>(mr, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    cqlQuery.setQuery("create columnfamily dollarbyuser (key 'org.apache.cassandra.db.marshal.UTF8Type' primary key) with  "
            + "comparator = 'org.apache.cassandra.db.marshal.UTF8Type' "
            + "and default_validation = 'CounterColumnType' ");
    QueryResult<CqlRows<String, String, String>> result = cqlQuery.execute();

    cqlQuery = new CqlQuery<String, String, String>(mr, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    cqlQuery.setQuery("create columnfamily itemcountbyuser (key 'org.apache.cassandra.db.marshal.UTF8Type' primary key) with  "
            + "comparator = 'org.apache.cassandra.db.marshal.UTF8Type' "
            + "and default_validation = 'CounterColumnType' ");
    result = cqlQuery.execute();

   
    Workload reducer = new Workload();
    reducer.active = true;
    reducer.consumerGroup = "group1";
    reducer.maxWorkers = 4;
    reducer.messageHandlerName = "com.jointhegrid.ironcount.mapreduce.ReduceHandler";

    reducer.name = "reduce";
    reducer.properties = new HashMap<String, String>();

    reducer.properties.put("mr.cas", "localhost:9157");
    reducer.properties.put("mr.ks", "mr");

    reducer.topic = "reduce";
    reducer.zkConnect = "localhost:8888";

    Workload mapper = new Workload();
    mapper.active = true;
    mapper.consumerGroup = "group1";
    mapper.maxWorkers = 4;
    mapper.messageHandlerName = "com.jointhegrid.ironcount.mapreduce.MapHandler";
    mapper.name = "map";
    mapper.properties = new HashMap<String, String>();
    mapper.properties.put("zk.connect", "localhost:8888");
    mapper.topic = "map";
    mapper.zkConnect = "localhost:8888";



    Properties p = System.getProperties();
    p.put(WorkloadManager.ZK_SERVER_LIST, "localhost:8888");
    WorkloadManager m = new WorkloadManager(p);
    m.init();

    m.applyWorkload(mapper);
    m.applyWorkload(reducer);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException ex) {
      Logger.getLogger(MapReduceJoinIntegrationTest.class.getName())
              .log(Level.SEVERE, null, ex);
    }

   // producer.send(new ProducerData<Integer, String>(topic, "1"));
   // producer.send(new ProducerData<Integer, String>(topic, "2"));
   // producer.send(new ProducerData<Integer, String>(topic, "3"));

    producer.send(new ProducerData<Integer, String>("map", "user|1:edward"));
    producer.send(new ProducerData<Integer, String>("map", "user|2:nate"));
    producer.send(new ProducerData<Integer, String>("map", "user|3:stacey"));

    producer.send(new ProducerData<Integer, String>("map", "cart|1:saw:2.00"));
    producer.send(new ProducerData<Integer, String>("map", "cart|1:hammer:3.00"));
    producer.send(new ProducerData<Integer, String>("map", "cart|3:puppy:1.00"));



    try {
      Thread.sleep(19000);
    } catch (InterruptedException ex) {
      Logger.getLogger(MapReduceJoinIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    CounterQuery<String, String> mcq = new ThriftCounterColumnQuery
            <String, String>(mr, StringSerializer.get(), StringSerializer.get());

    mcq.setKey("edward");
    mcq.setColumnFamily("dollarbyuser");
    mcq.setName("spent");

    QueryResult<HCounterColumn<String>> res = mcq.execute();
    Assert.assertEquals(new Long(5), res.get().getValue());

  }
}
