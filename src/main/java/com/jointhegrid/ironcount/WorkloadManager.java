package com.jointhegrid.ironcount;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;
import org.scale7.zookeeper.cages.ZkCagesException;
import org.scale7.zookeeper.cages.ZkMultiPathLock;
import org.scale7.zookeeper.cages.ZkSessionManager;

/**
 * @author zznate
 */
public class WorkloadManager implements Watcher {

  final static Logger logger = Logger.getLogger(WorkloadManager.class.getName());
  private ZooKeeper zk;
  private ZkSessionManager session;

  private ExecutorService executor;
  private int threadPoolSize = 4;
  private AtomicBoolean active;
  private UUID myId;//get this from properties
  private Properties props;

  private Map<WorkerThread,Object> workerThreads;

  public static final String ZK_SERVER_LIST="ic.zk.servers";


  public WorkloadManager(Properties p) {
    this.active = new AtomicBoolean(false);
    props = p;
    myId = UUID.randomUUID();
    workerThreads = new HashMap<WorkerThread,Object>();
  }

  public void init() {
    active.set(true);
    executor = Executors.newFixedThreadPool(threadPoolSize);
    try {
      zk = new ZooKeeper(props.getProperty( ZK_SERVER_LIST), 100, this);
      session = new ZkSessionManager( props.getProperty(ZK_SERVER_LIST));
      session.initializeInstance( props.getProperty(ZK_SERVER_LIST));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    try {
      if (zk.exists("/ironcount", true) == null) {
         zk.create("/ironcount", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (zk.exists("/ironcount/workers", false) == null) {
         zk.create("/ironcount/workers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (zk.exists("/ironcount/workloads", this) == null) {
         zk.create( "/ironcount/workloads", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      zk.create("/ironcount/workers/"+myId.toString(), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      zk.exists("/ironcount/workloads", this);
      System.out.println("created");

      List<String> children = zk.getChildren("/ironcount/workloads", false);
      considerStarting(children);
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void shutdown() {
    active.set(false);
    executor.shutdown();
  }

  @Override
  public void process(WatchedEvent we) {
    System.out.println("event " + we);
    if (we.getType() == we.getType().NodeCreated) {
      try {
        if (we.getPath().equals("/ironcount/workloads")){
          List<String> children = zk.getChildren("/ironcount/workloads", this);
          considerStarting(children);
        }
      } catch (KeeperException ex) {
        throw new RuntimeException(ex);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
    if (we.getType() == we.getType().NodeDeleted){
      if (we.getPath().startsWith("/ironcount/workloads")){
        stopWorkerThreadIfRunning(we.getPath());
      }
    }
    if (we.getType() == we.getType().NodeChildrenChanged) {
      if (we.getPath().equals("/ironcount/workloads")) {
        try {
          //new workloads have been added NOT DELETED
          List<String> children = zk.getChildren("/ironcount/workloads", this);
          considerStarting(children);

        } catch (KeeperException ex) {
          throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  public void stopWorkerThreadIfRunning(String child) {
    String [ ] parts = child.split("/");
    for (String part:parts){
      System.out.println(part);
    }
    //"/ironcount/workloads/workload"
    String name = parts[3];
    for (WorkerThread wt : this.workerThreads.keySet()){
      if (wt.workload.name.equals(name)){
        wt.executor.shutdown();
        wt.goOn=false;
      }
    }
  }

  public void considerStarting(List<String> workloads){
    System.out.println("consider starting");
    if (this.workerThreads.size()>=this.threadPoolSize){
      logger.warn("Already at thread pool size wont start a worker");
      return;
    }
    System.out.println(workloads);
    for (String workload: workloads){
      try {
        Stat s = zk.exists("/ironcount/workloads/" + workload, false);
        byte[] b = zk.getData("/ironcount/workloads/" + workload, false, s);
        Workload w = this.deserializeWorkload(b);
        boolean alreadyRunning=false;
        for (WorkerThread wt:this.workerThreads.keySet()){
          if (wt.workload.name.equals(w.name)){
            alreadyRunning=true;
          }
        }
        if (alreadyRunning){
          continue;
        } else {
          considerStartingWorkload(w);
        }
      } catch (KeeperException ex) {
       throw new RuntimeException(ex);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public void considerStartingWorkload(Workload w){
      System.out.println("consider startingWorkload");
    
    ZkMultiPathLock lock = new ZkMultiPathLock();
    lock.addWriteLock("/ironcount/workloads/"+w.name);
    try {
      lock.acquire();
      List<String> children = zk.getChildren("/ironcount/workloads/" + w.name, false);
      if (children.size() <= w.maxWorkers){
        WorkerThread wt = new WorkerThread(this,w);
        this.executor.submit(wt);
        this.workerThreads.put(wt, new Object());
        System.out.println("Put job on executor thread");
      }
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    } catch (ZkCagesException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    } finally {
      lock.release();
    }
  }

  public byte[] serializeWorkload(Workload w) {
    ObjectMapper map = new ObjectMapper();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      map.writeValue(baos, w);
    } catch (IOException ex) {
      System.out.println(ex);
    }
    return baos.toByteArray();
  }

  public Workload deserializeWorkload(byte[] b) {
    ObjectMapper m = new ObjectMapper();
    JavaType t = TypeFactory.type(Workload.class);
    Workload work = null;
    try {
      work = (Workload) m.readValue(new String(b), t);
    } catch (IOException ex) {
      System.out.println(ex);
    }
    return work;
  }

  public UUID getMyId() {
    return myId;
  }

  public void setMyId(UUID myId) {
    this.myId = myId;
  }

  public void startWorkload(Workload w){
    try {
      //byte[] b = zk.getData("/ironcount/workloads/" + workload, false, s);
      zk.create("/ironcount/workloads/" + w.name, this.serializeWorkload(w),
              Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      System.out.println("created node");
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }


  public void stopWorkload(Workload w){
    try {
      //byte[] b = zk.getData("/ironcount/workloads/" + workload, false, s);
      Stat s = zk.exists("/ironcount/workloads/" + w.name, false);
      zk.setData("/ironcount/workloads/" + w.name, this.serializeWorkload(w), s.getVersion());
      System.out.println("stopWorkload");
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void deleteWorkload(Workload w) {
    
    try {

      List<String> children = zk.getChildren("/ironcount/workloads/" + w.name, false);
      while (children.size()>0){
        System.out.println("waiting for children of workload to shutdown");
        Thread.sleep(1000);
        children = zk.getChildren("/ironcount/workloads/" + w.name, false);
      }

      Stat s = zk.exists("/ironcount/workloads/" + w.name, false);
      Thread.sleep(2000);
      zk.delete("/ironcount/workloads/" + w.name, s.getVersion());
    } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    }
  }
  public Properties getProps() {
    return props;
  }

  public void setProps(Properties props) {
    this.props = props;
  }

  public Map<WorkerThread, Object> getWorkerThreads() {
    return workerThreads;
  }

  public void setWorkerThreads(WeakHashMap<WorkerThread, Object> workerThreads) {
    this.workerThreads = workerThreads;
  }
 
}