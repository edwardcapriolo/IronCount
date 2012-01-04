package com.jointhegrid.ironcount.mockingbird;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.jointhegrid.ironcount.IronIntegrationTest;
import com.jointhegrid.ironcount.IronWorker;
import com.jointhegrid.ironcount.Workload;
import kafka.javaapi.producer.ProducerData;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import org.junit.Test;

/**
 *
 * @author edward
 */
public class MockingBirdIntegrationTest extends IronIntegrationTest {

  // TODO add test methods here.
  // The methods must be annotated with annotation @Test. For example:
  //
  // @Test
 @Test
  public void mockingBirdDemo() {

   // set up the column families to save data in
   KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition("mockingbird");
   ColumnFamilyDefinition cfwork = HFactory.createColumnFamilyDefinition
           ("mockingbird", "mockingbird", ComparatorType.UTF8TYPE);
   cluster.addKeyspace(ksDef, true);
   Keyspace moch = HFactory.createKeyspace("mockingbird", cluster);
   CqlQuery<String,String,String> cqlQuery = new CqlQuery<String,String,String>
           (moch,StringSerializer.get(),StringSerializer.get(),StringSerializer.get());
   cqlQuery.setQuery("create columnfamily mockingbird (key 'CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)' primary key) with  "+
  "comparator = 'org.apache.cassandra.db.marshal.UTF8Type' "+
  "and default_validation = 'CounterColumnType' " );
  QueryResult<CqlRows<String,String,String>> result = cqlQuery.execute();



    Workload w = new Workload();
    w.active=true;
    w.consumerGroup="group1";
    w.maxWorkers=4;
    w.messageHandlerName="com.jointhegrid.ironcount.mockingbird.MockingBirdMessageHandler";
    w.name="testworkload";
    w.properties=new HashMap<String,String>();
    w.topic=topic;
    w.zkConnect="localhost:8888";

    //pass the cassandra information to the IronWorker
    w.properties.put("mocking.ks", "mockingbird");
    w.properties.put("mocking.cf", "mockingbird");
    w.properties.put("mocking.cas","localhost:9157");
    

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

    producer.send(new ProducerData<Integer, String>(topic, "http://www.ed.com/stuff"));
    producer.send(new ProducerData<Integer, String>(topic, "http://toys.ed.com/toys"));
    producer.send(new ProducerData<Integer, String>(topic, "http://www.ed.com/"));
    try {
      Thread.sleep(8000);
    } catch (InterruptedException ex) {
      Logger.getLogger(IronIntegrationTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    //SimpleMessageHandler h = new SimpleMessageHandler();
    //Assert.assertEquals(2, h.messageCount.get());
    //Assert.assertEquals(true, h.handlerCount.get()>=2);

    iw.stop();
  }
}
