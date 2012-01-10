package com.jointhegrid.ironcount;

import java.util.List;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import me.prettyprint.hector.api.Cluster;

/**
 * @author zznate
 */
public class IroncountWorkloadManager {

  private final Cluster cluster;
  // responsibilities:
  // - control the ExecutorService
  // - manage WorkerThread lifecycle
  private ExecutorService executor;
  private ScheduledExecutorService scheduledExector;
  private int threadPoolSize = 4;
  private DataLayer dataLayer;
  private AtomicBoolean active;
  private UUID myId;

  private WeakHashMap<WorkerThread,Object> workerThreads;

  public IroncountWorkloadManager(Cluster cluster) {
    this.cluster = cluster;
    this.dataLayer = new DataLayer(cluster);
    this.active = new AtomicBoolean(false);
    myId = UUID.randomUUID();
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
    for (Workload workload : workloads) {
      // get the jobInfo for each workload
      JobInfo ji = dataLayer.getJobInfoForWorkload(workload);
      if (workload.active==false){
        // if this Manager is running any task of this type shut it down
        for (WorkerThread wt:this.workerThreads.keySet()){
          if (wt.workload.name.equals(workload.name)){
            wt.goOn=false;
          }
        }
      } else if (ji.workerIds.size() < workload.maxWorkers &&
         (!ji.workerIds.contains(this.myId.toString())) ) {
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
