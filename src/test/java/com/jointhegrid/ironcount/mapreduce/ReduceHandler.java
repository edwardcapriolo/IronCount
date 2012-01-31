package com.jointhegrid.ironcount.mapreduce;

import com.jointhegrid.ironcount.MessageHandler;
import com.jointhegrid.ironcount.WorkerThread;
import com.jointhegrid.ironcount.Workload;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import kafka.message.Message;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class ReduceHandler implements MessageHandler {

  HashMap<User,ArrayList<Item>> data = new HashMap<User,ArrayList<Item>>();
  Cluster cluster;
  Keyspace keyspace;
  Workload w;

  public ReduceHandler(){

  }

  @Override
  public void setWorkload(Workload w) {
    this.w = w;
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

    if (table.equals("user")) {
      User u = new User();
      u.parse(columns);
      if (! data.containsKey(u)){
        data.put(u, new ArrayList<Item>());
        System.out.println("added user "+u.name + " "+u.id);
      }
    } else if ( table.equals("cart")){

      Item i = new Item();
      i.parse(columns);
      System.out.println("adding car " + i.itemName + " "+i.userfk);
      for (User u : data.keySet()){
        if (u.id==i.userfk){
          data.get(u).add(i);
          //counter (items for user)
          incrementItemCounter(u);
          //count ($ spent by user)
          incrementDollarByUser(u,i);
          System.out.println("done with increments");
        }
      }
    }
  }


  public void incrementItemCounter(User u){
    try {
      Mutator<String> mut = HFactory.createMutator(keyspace,
              StringSerializer.get());
      HCounterColumn<String> hc = HFactory.createCounterColumn("count", 1L);
      mut.addCounter(u.name, "itemcountbyuser", hc);
      mut.execute();
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }

  public void incrementDollarByUser(User u, Item i){
    try {
      Mutator<String> mut = HFactory.createMutator(keyspace,
              StringSerializer.get());
      HCounterColumn<String> hc = HFactory.createCounterColumn("spent", i.price.intValue());
      mut.addCounter(u.name, "dollarbyuser", hc);
      mut.execute();
    } catch (Exception ex) {
      System.out.println(ex);
    }
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
