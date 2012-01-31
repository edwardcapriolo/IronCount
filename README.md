IronCount
=============

IronCount provides a framework to manage consumers of Kafka message queues across multiple nodes.

Components
-----
IronCount works with three components. Two components are external Kafka and ZooKeeper. IronCount
has one component WorkloadManager that runs on multiple machines. 

Workloads
-----

A Workload is an object that stores several pieces of information.

* name: System wide unique name for the Workload
* topic: Name of the Kafka topic this handler will get data from
* messageHandlerName: Class that implements MessageHandler interface 
* zkConnect: Information to connect to zookeeper
* maxWorkers: Maximum number of instances of MessageHander to run across the cluster
* properties: A map of properties that can be used for configuration
* active: a flag to start or pause workloads

In a serialized from it looks like this:

    {"name":"testworkload"
      ,"topic":"topic1"
      ,"consumerGroup":"group1"
      ,"messageHandlerName":"com.jointhegrid.ironcount.eventtofile.MessageToFileHandler"
      ,"zkConnect":"localhost:2181"
      ,"maxWorkers":4
      ,"properties":{"aprop":"avalue"}
      ,"active":true
    }

To start a Workload create a JSON clob like the one above and save it to a file. Then use
Deploy tool to write this entry to ZooKeeper. At this point WorkloadManagers should notice
the changes to zookeeper and start instances of the Workload.

Extending
-----

The first step is to implement the `MessageHandler` interface. Each worker instantates the handler
once. Then each kafka message is passed to the `handleMessage(Message m)` method. 

    package com.jointhegrid.ironcount.mockingbird;

    import com.jointhegrid.ironcount.MessageHandler;
    import com.jointhegrid.ironcount.WorkerThread;
    import com.jointhegrid.ironcount.Workload;
    import kafka.message.Message;

    public class MessageHandlerExt implements MessageHandler{

      public MessageHandlerExt(){}

      @Override
      public void setWorkload(Workload w) {
      }

      @Override
      public void handleMessage(Message m) {
      }

      @Override
      public void setWorkerThread(WorkerThread wt) {
      }
    }

Demos
-----

IronCount has some build in demo's to show it's usefulness. The first is MockingBird, which offers Rainbird 
style URL counting, and data persistance to Cassandra. See `com.jointhegrid.ironcount.mockingbird.*` in the 
test packages.

    public void handleMessage(Message m) {
      String url = getMessage(m);
      URI i = URI.create(url);
      String domain = i.getHost();
      String path = i.getPath();
      String[] parts = domain.split("\\.");
      Stack<String> s = new Stack<String>();
      s.add(path);
      s.addAll(Arrays.asList(parts));
      StringBuilder sb = new StringBuilder();

      for (int j = 0; j <= parts.length; j++) {
        sb.append(s.pop());
        countIt(sb.toString());//write to C*
        sb.append(":");
      }
    }

The framework takes care of the transport and queuing and allows the user to focus on application logic.

The second demo is a Join similar to s4's join demo. This one is implemented with two Kafka queues,
a queue named `map` and a queue named `reduce`. The `MapHandler` handlesMessages from the map queue, processes
them and then send them to the reduce queue. Kafka has the notion of partitioners and the join key
is used internally to route messages for the same user_id to the same handler.  The ReduceHandler writes
partial aggreggations as Cassandra counters made possible by Kafka's underlying partitioning. This system 
where one Workload creates data for another can be viewed as a pipe or a feedback loop.

    @Override
    public void setWorkload(Workload w) {
      this.w=w;
      producerProps = new Properties();
      producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
      producerProps.put("zk.connect", w.properties.get("zk.connect"));
      producerConfig = new ProducerConfig(producerProps);
      producer = new Producer<String,String>(producerConfig);
    }

    @Override
    public void handleMessage(Message m) {
      //message looks like this
      //users|1:edward
      //or
      //cart|1:saw
      String line = getMessage(m);
      String[] parts = line.split("\\|");
      String table = parts[0];
      String row = parts[1];
      String [] columns = row.split(":");

      //results look like this
      //Partitioner (1) users|1:edward
      //or
      //partitioner (1) cart|1:saw

      producer.send(new ProducerData<String, String>
        ("reduce", columns[0], Arrays.asList(table+"|"+row)));
    }

 Kafka has the notion of partitioners and the join key is used internally to route 
messages for the same user_id to the same handler.  The ReduceHandler writes
partial aggreggations as Cassandra counters made possible by Kafka's underlying partitioning. This system 
where one Workload creates data for another can be viewed as a pipe or a feedback loop.

    @Override
    public void handleMessage(Message m) {
      String line = getMessage(m);
      String[] parts = line.split("\\|");
      String table = parts[0];
      String row = parts[1];
      String [] columns = row.split(":");

      if (table.equals("user")) {
        User u = new User();
        u.parse(columns);
        if (! data.containsKey(u)){
          data.put(u, new ArrayList<Item>());
        }
      } else if ( table.equals("cart")){
        Item i = new Item();
        i.parse(columns);
        for (User u : data.keySet()){
          if (u.id==i.userfk){
            data.get(u).add(i);
            //counter (items for user)
            incrementItemCounter(u);
            //count ($ spent by user)
            incrementDollarByUser(u,i);
          }
        }
      }
    }

See `com.jointhegrid.ironcount.mapreduce` for example code.
