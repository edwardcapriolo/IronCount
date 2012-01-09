package com.jointhegrid.ironcount;

import java.util.List;
import java.util.UUID;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

public class IronWorker implements Runnable {

  public int slotcount = 4;
  public ThreadGroup slots;
  public Cluster cluster;
  public UUID myId;
  public boolean goOn;
  public DataLayer dl;

  public IronWorker() {
    goOn=true;
    myId = UUID.randomUUID();
    slots = new ThreadGroup("slots") {

      @Override
      public void uncaughtException(Thread t, Throwable e) {
        System.err.println(e);
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public void run() {
    // de-couple the task, entry point and execution of said task into 3 different components
    cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9157");
    dl = new DataLayer(cluster);
    while (goOn) {
      if (slots.activeCount() < this.slotcount) {
        // get all the workloads
        List<Workload> workloads = dl.getWorkloads();
        for (Workload workload : workloads) {
          // get the jobInfo for each workload
          JobInfo ji = dl.getJobInfoForWorkload(workload);
          // if there are "slots" open for the workload,
          // start a worker
          // otherwise, continue
          if (ji.workerIds.size() < workload.maxWorkers) {
            if (! ji.workerIds.contains(this.myId.toString())){
              startWorker(workload);
            } else {
              System.out.println("already running a workload of this type on this node");
            }
          } else {
            System.out.println("more workers then maxWorkers. Will not start");
          }


        }
      }
      sleep(1);
    }
  }

  public void startWorker(Workload w){
    WorkerThread wt = new WorkerThread(w);
    Thread t = new Thread(this.slots, wt);
    t.setDaemon(false);
    t.start();
    dl.registerJob(w, wt);
  }

  public void sleep(long millis) {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
    }
  }

  public void stop(){
    this.goOn=false;
  }

  public void runDaemon(){
    ThreadGroup tg = new ThreadGroup("master"){
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        System.out.println(e);
        throw new RuntimeException(e);
      }
    };
    Thread t = new Thread(tg,this);
    t.setDaemon(false);
    t.start();
  }
  
  public static void main(String[] args) {
    IronWorker iw = new IronWorker();
    iw.runDaemon();
  }
}