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
package com.jointhegrid.ironcount.manager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.recipes.lock.WriteLock;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;


/**
 * @author zznate
 */
public class WorkloadManager implements Watcher,WorkloadManagerMBean {

  public static final String MBEAN_OBJECT_NAME = "com.jointhegrid.ironcount:type=WorkloadManager";

  final static Logger logger = Logger.getLogger(WorkloadManager.class.getName());
  private ZooKeeper zk;
  private ExecutorService executor;
  private int threadPoolSize = 4;
  private AtomicBoolean active;
  private UUID myId;//get this from properties
  private Properties props;
  private long rescanMillis=2000;

  private Map<WorkerThread,Object> workerThreads;

  public static final String ZK_SERVER_LIST="ic.zk.servers";
  public static final String IC_THREAD_POOL_SIZE="ic.thread.pool.size";

  public WorkloadManager(Properties p) {
    this.active = new AtomicBoolean(false);
    props = p;
    myId = UUID.randomUUID();
    workerThreads = new HashMap<WorkerThread,Object>();
    if (p.contains(IC_THREAD_POOL_SIZE)){
      this.threadPoolSize = Integer.parseInt(IC_THREAD_POOL_SIZE);
    }
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    try {
      mbs.registerMBean(this, new ObjectName(MBEAN_OBJECT_NAME+",uuid="+myId));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void init() {
    active.set(true);
    executor = Executors.newFixedThreadPool(threadPoolSize);
    try {
      zk = new ZooKeeper(props.getProperty( ZK_SERVER_LIST), 100, this);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    try {
      createICHeir();
      zk.create("/ironcount/workers/"+myId.toString(), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      zk.exists("/ironcount/workloads", this);
      new Thread(){
        public void run(){
          while (true){
            try {
              List<String> children = zk.getChildren("/ironcount/workloads", false);
              considerStarting(children);
              Thread.sleep(rescanMillis);
            } catch (Exception ex){
              logger.error("Exception during scan "+ex);
            }
          }
        }
      }.start();
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void createICHeir() throws KeeperException, InterruptedException {
    if (zk.exists("/ironcount", true) == null) {
      logger.info("Creating /ironcount heirarchy");
      zk.create("/ironcount", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    if (zk.exists("/ironcount/workers", false) == null) {
      zk.create("/ironcount/workers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    if (zk.exists("/ironcount/workloads", this) == null) {
      zk.create("/ironcount/workloads", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  public void shutdown() {
    active.set(false);
    for (WorkerThread wt : this.workerThreads.keySet()){
      wt.goOn=false;
      wt.terminate();
    }
    executor.shutdown();
  }

  @Override
  public void process(WatchedEvent we) {
    logger.debug(we);
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
    //"/ironcount/workloads/workload"
    String name = parts[3];
    for (WorkerThread wt : this.workerThreads.keySet()){
      if (wt.workload.name.equals(name)){
        wt.goOn=false;
        wt.terminate();
      }
    }
  }

  public void considerStarting(List<String> workloads){
    
    if (this.workerThreads.size()>=this.threadPoolSize){
      logger.warn("Already at thread pool max size. Will not start a worker");
      return;
    }
    logger.debug("consider starting "+ workloads);
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
    logger.debug("considert starting "+w);
    if (w.active==false){
      logger.debug(w.name + " is non active");
      return;
    }
    WriteLock l = null;
    try {
      l = new WriteLock(zk, "/ironcount/workloads/" + w.name,null);
      l.lock();
      List<String> children = zk.getChildren("/ironcount/workloads/" + w.name, false);
      if (children.size() <= w.maxWorkers){
        WorkerThread wt = new WorkerThread(this,w);
        this.executor.submit(wt);
        this.workerThreads.put(wt, new Object());
        logger.debug("Started worker thread "+wt+ " "+w);
      }
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    } catch (Throwable t){
      t.printStackTrace(System.err);
      logger.error(t);
      throw new RuntimeException (t);
    } finally {
      l.unlock();
    }
  }

  public byte[] serializeWorkload(Workload w) {
    ObjectMapper map = new ObjectMapper();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      map.writeValue(baos, w);
    } catch (IOException ex) {
      logger.error(ex);
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
      logger.error(ex);
    }
    return work;
  }

  public UUID getMyId() {
    return myId;
  }

  public void setMyId(UUID myId) {
    this.myId = myId;
  }

  public void applyWorkload(Workload w){
    try {
      Stat s = zk.exists("/ironcount/workloads/" + w.name, false);
      if (s!=null){
        zk.setData("/ironcount/workloads/" + w.name, this.serializeWorkload(w), s.getVersion());
      } else {
        zk.create("/ironcount/workloads/" + w.name, this.serializeWorkload(w),
              Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void applyWorkload(String workload){
    Workload w = this.deserializeWorkload(workload.getBytes());
    applyWorkload(w);
  }

  public List<Workload> getAllWorkloads() {
    List<Workload> all = new ArrayList<Workload>();
    try {
      List<String> children = zk.getChildren("/ironcount/workloads", false);
      for (String child : children) {
        Stat s = zk.exists("/ironcount/workloads/" + child, false);
        byte[] b = zk.getData("/ironcount/workloads/" + child, false, s);
        Workload w = this.deserializeWorkload(b);
        all.add(w);
      }
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    return all;
  }

  public void deleteWorkload(Workload w) {
    
    try {
      List<String> children = zk.getChildren("/ironcount/workloads/" + w.name, false);
      while (children.size()>0){
        logger.debug("Waiting for child shutdown "+w);
        Thread.sleep(1000);
        children = zk.getChildren("/ironcount/workloads/" + w.name, false);
      }
      Stat s = zk.exists("/ironcount/workloads/" + w.name, false);
      Thread.sleep(1000);
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

  @Override
  public void setRescanMillis(long millis) {
    this.rescanMillis=millis;
  }

  @Override
  public long getRescanMillis() {
    return rescanMillis;
  }

  @Override
  public void setThreadPoolSize(int size) {
    this.threadPoolSize=size;
  }

  @Override
  public int getThreadPoolSize() {
    return threadPoolSize;
  }

  @Override
  public List<String> getConfiguredWorkloadNames(){
    List<String> result  = new ArrayList<String>();
    List<Workload> wlist = this.getAllWorkloads();
    for (Workload w:wlist){
      result.add(w.name);
    }
    String [] names = new String[result.size()];
    for (int i =0;i<names.length;i++){
      names[i]=result.get(i);
    }
    return result;
  }

  @Override
  public String getWorkloadAsJSON(String workloadName) {
    for (Workload w:getAllWorkloads()){
      if (w.name.equals(workloadName)){
        return new String(this.serializeWorkload(w));
      }
    }
    return null;
  }

  @Override
  public List<String> getInstancesRunningWorkload(String workloadName) {
    List<String> results = new ArrayList<String>();
    try {
      for (Workload w : getAllWorkloads()) {
        if (w.name.equals(workloadName)){
         results.addAll(zk.getChildren("/ironcount/workloads/" + w.name, false));
        }
      }
    } catch (KeeperException ex) {
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    return results;
  }
}