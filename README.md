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


