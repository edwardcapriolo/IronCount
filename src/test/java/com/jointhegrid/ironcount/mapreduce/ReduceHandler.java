package com.jointhegrid.ironcount.mapreduce;

import com.jointhegrid.ironcount.MessageHandler;
import com.jointhegrid.ironcount.WorkerThread;
import com.jointhegrid.ironcount.Workload;
import com.jointhegrid.ironcount.mockingbird.CounterBatcher;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import kafka.message.Message;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class ReduceHandler implements MessageHandler {

  HashMap<String,ArrayList<String>> data = new HashMap<String,ArrayList<String>>();
    Cluster cluster ;
  Keyspace keyspace;
  Workload w;

  public ReduceHandler(){

  }

  @Override
  public void setWorkload(Workload w) {
    this.w=w;
     cluster = HFactory.getOrCreateCluster("mr", w.properties.get("mr.cas"));
    keyspace = HFactory.createKeyspace(w.properties.get("mr.ks"), cluster);
  }

  @Override
  public void handleMessage(Message m) {
    String line = getMessage(m);
    System.out.println("reduce line "+line);
    String[] parts = line.split("\\|");
    String table = parts[0];
    String row = parts[1];
    String [] columns = row.split(":");

    //id                                  //name      //cost
    if (data.containsKey(columns[0])){
       data.get(columns[0]).add(table+":"+columns[1]);
    } else {
      data.put(columns[0], new ArrayList<String>());
      data.get(columns[0]).add(table+":"+columns[1]);
    }

    for (Map.Entry<String,ArrayList<String>> entry: data.entrySet()){
      List<String> citems = new ArrayList<String>();
      for (String s : entry.getValue()){
        if ( s.split(":")[0].equals("cart") ){
          citems.add( s.split(":")[1] );
        }
      }
      String name=null;
      for (String s : entry.getValue()){
        if ( s.split(":")[0].equals("users") ){
          name= s.split(":")[1] ;
        }
      }

      try {
        for (String item:citems){
          Mutator<String> mut = HFactory.createMutator(keyspace,
                  StringSerializer.get());
          HCounterColumn<String> hc = HFactory.createCounterColumn(item, 1L);
          mut.addCounter(name, w.properties.get("mr.cf"), hc);
          mut.execute();
        }
      } catch (Exception ex) {
        System.out.println(ex);
      }

    }

    //System.out.println("reducer line:"+line);
    //producer.send(new ProducerData<String, String>
    //        ("reduce", key, Arrays.asList(value)));
  }

  @Override
  public void setWorkerThread(WorkerThread wt) {
    
  }

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
