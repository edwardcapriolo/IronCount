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
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public void run() {

    cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9157");
    dl = new DataLayer(cluster);
    while (goOn) {
      if (slots.activeCount() < this.slotcount) {
        List<Workload> workloads = dl.getWorkloads();
        System.err.println("workloads size " + workloads.size());
        for (Workload workload : workloads) {
          JobInfo ji = dl.getJobInfoForWorkload(workload);
          if (ji.workerIds.size() < workload.maxWorkers) {
            System.err.println("workers:" +ji.workerIds.size() +" max:"+ workload.maxWorkers);
            System.out.println("trying to sart");
            startWorker(workload);
          } else {
            System.out.println("more workers then maxWorkers. Will not start");
          }
        }
      }
      sleep(1);
    }
    System.err.println("ending run loop");
  }

  public void startWorker(Workload w){
    System.out.println("startWorker start");
    WorkerThread wt = new WorkerThread(this,w);
    Thread t = new Thread(this.slots, wt);
    t.setDaemon(false);
    t.start();
    dl.registerJob(w, this);
    System.out.println("startWorker end");
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