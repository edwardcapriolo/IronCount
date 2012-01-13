package com.jointhegrid.ironcount;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import me.prettyprint.hector.api.Cluster;
import org.apache.log4j.Logger;

/**
 * @author zznate
 */
public class IroncountWorkloadManager {

  final static Logger logger = Logger.getLogger(IroncountWorkloadManager.class.getName());
  private final Cluster cluster;
  // responsibilities:
  // - control the ExecutorService
  // - manage WorkerThread lifecycle
  private ExecutorService executor;
  private ScheduledExecutorService scheduledExector;
  private int threadPoolSize = 4;
  private DataLayer dataLayer;
  private AtomicBoolean active;
  private UUID myId;//get this from properties
  private Properties props;

  private WeakHashMap<WorkerThread,Object> workerThreads;

  public IroncountWorkloadManager(Cluster cluster) {
    this.cluster = cluster;
    this.dataLayer = new DataLayer(cluster);
    this.active = new AtomicBoolean(false);
    props = System.getProperties();
    if (props.contains("workermanager.uid")){
      myId = UUID.fromString((String)props.get("workermanager.uid"));
      logger.warn("UUID from props file "+myId);
    } else {
      myId = UUID.randomUUID();
      logger.warn("random UUID generated "+myId);
    }
    workerThreads = new WeakHashMap<WorkerThread,Object>();
  }

  public void init() {
    active.set(true);
    executor = Executors.newFixedThreadPool(threadPoolSize);
    scheduledExector = Executors.newScheduledThreadPool(1);
  }

  public void shutdown() {
    active.set(false);
    scheduledExector.shutdown();
    executor.shutdown();
  }

  public void execute() {
    final Runnable runner = new Runnable() {
      @Override
      public void run() {
        executeInternal();
      }
    };
    scheduledExector.scheduleWithFixedDelay(runner, 0, 2, TimeUnit.SECONDS);
  }

  /**
   * Submits to pool
   */
  private void executeInternal() {
    // while true...
    // get all the workloads
    List<Workload> workloads = dataLayer.getWorkloads();
    logger.warn("workload size "+ workloads.size());
    for (Workload workload : workloads) {
      // get the jobInfo for each workload
      JobInfo ji = dataLayer.getJobInfoForWorkload(workload);
      logger.warn("current number of workers "+ ji.workerIds.size());
      if (workload.active==false){
        // if this Manager is running any task of this type shut it down
        for (WorkerThread wt:this.workerThreads.keySet()){
          if (wt.workload.name.equals(workload.name)){
            wt.goOn=false;
            //wait for state to hit done?
            //while (wt.status != WorkerThread.WorkerThreadStatus.DONE){
            //  System.err.println("waiting for done. Currently "+wt.status);
            //}
            //System.err.println("Currently "+wt.status);
            dataLayer.deregisterJob(this, workload, wt);
          }
        }
      } else if (ji.workerIds.size() < workload.maxWorkers &&
         (!ji.workerIds.contains(this.myId.toString())) ) {
        logger.warn("Starting instance of "+workload);
        //we do not want to run more then one instance of a Workload because
        //JobInfo can not handle that. This should also naturally spread tasks
        WorkerThread wt = new WorkerThread(workload);
        dataLayer.registerJob(this,workload,wt);
        this.workerThreads.put(wt, new Object());
        // this executor service is capped at threadPoolSize threads, but has
        // unbounded queuing.
        // TODO switch to ThreadPoolExecutor with an ArrayBlockingQueue and a
        // rejected execution handler
        executor.submit(wt);
      }
    }
  }

  public UUID getMyId() {
    return myId;
  }

  public void setMyId(UUID myId) {
    this.myId = myId;
  }
  
}
