/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jointhegrid.ironcount.mapreduce;

import com.jointhegrid.ironcount.IronIntegrationTest;
import com.jointhegrid.ironcount.Workload;
import com.jointhegrid.ironcount.WorkloadManager;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.javaapi.producer.ProducerData;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import org.junit.Test;

/**
 *
 * @author edward
 */
public class MapReduceJoinIntegrationTest extends IronIntegrationTest {

  @Test
  public void mapReduceJoin() {

    KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition("mockingbird");
    cluster.addKeyspace(ksDef, true);
    Keyspace moch = HFactory.createKeyspace("mockingbird", cluster);
    CqlQuery<String, String, String> cqlQuery = new CqlQuery<String, String, String>(moch, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    cqlQuery.setQuery("create columnfamily mrjoin (key 'org.apache.cassandra.db.marshal.UTF8Type' primary key) with  "
            + "comparator = 'org.apache.cassandra.db.marshal.UTF8Type' ");
    QueryResult<CqlRows<String, String, String>> result = cqlQuery.execute();
   
    Workload reducer = new Workload();
    reducer.active = true;
    reducer.consumerGroup = "group1";
    reducer.maxWorkers = 4;
    reducer.messageHandlerName = "com.jointhegrid.ironcount.mapreduce.ReduceHandler";

    reducer.name = "reduce";
    reducer.properties = new HashMap<String, String>();
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


        Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "com.jointhegrid.ironcount.SimpleMessageHandler";
    w.name = "testworkload";
    w.properties = new HashMap<String, String>();
    w.topic = topic;
    w.zkConnect = "localhost:8888";

    Properties p = System.getProperties();
    p.put(WorkloadManager.ZK_SERVER_LIST, "localhost:8888");
    WorkloadManager m = new WorkloadManager(p);
    m.init();

    m.startWorkload(mapper);
    m.startWorkload(reducer);
    m.startWorkload(w);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ex) {
      Logger.getLogger(MapReduceJoinIntegrationTest.class.getName())
              .log(Level.SEVERE, null, ex);
    }

   // producer.send(new ProducerData<Integer, String>(topic, "1"));
   // producer.send(new ProducerData<Integer, String>(topic, "2"));
   // producer.send(new ProducerData<Integer, String>(topic, "3"));

    producer.send(new ProducerData<Integer, String>("map", "users|1:edward"));
    producer.send(new ProducerData<Integer, String>("map", "users|2:nate"));
    producer.send(new ProducerData<Integer, String>("map", "users|3:stacey"));

    producer.send(new ProducerData<Integer, String>("map", "cart|1:saw:2.00"));
    producer.send(new ProducerData<Integer, String>("map", "cart|1:hammer:3.00"));
    producer.send(new ProducerData<Integer, String>("map", "cart|3:puppy:1.00"));



    try {
      Thread.sleep(19000);
    } catch (InterruptedException ex) {
      Logger.getLogger(MapReduceJoinIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
