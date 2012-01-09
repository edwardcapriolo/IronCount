package com.jointhegrid.ironcount;

import java.util.List;
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

  public IroncountWorkloadManager(Cluster cluster) {
    this.cluster = cluster;
    this.dataLayer = new DataLayer(cluster);
    this.active = new AtomicBoolean(false);
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
    scheduledExector.scheduleWithFixedDelay(runner, 0, 5, TimeUnit.SECONDS);
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
      if (ji.workerIds.size() < workload.maxWorkers) {
        WorkerThread wt = new WorkerThread(workload);
        dataLayer.registerJob(workload,wt);
        // this executor service is capped at threadPoolSize threads, but has
        // unbounded queuing.
        // TODO switch to ThreadPoolExecutor with an ArrayBlockingQueue and a
        // rejected execution handler
        executor.submit(wt);
      }
    }
  }
}
