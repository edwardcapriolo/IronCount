package com.jointhegrid.ironcount;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import me.prettyprint.hector.api.Cluster;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;

public class DeployTool {

  final static Logger logger = Logger.getLogger(ICLauncher.class.getName());
  private static Properties properties;
  private static WorkloadManager workloadManager;

  public static void main(String[] args) {

    properties = System.getProperties();
    if (properties.get(WorkloadManager.ZK_SERVER_LIST) == null) {
      logger.warn(WorkloadManager.ZK_SERVER_LIST+" was not defined setting to localhost:2181");
      properties.setProperty(WorkloadManager.ZK_SERVER_LIST, "localhost:2181");
    }

    workloadManager = new WorkloadManager(properties);
    workloadManager.init();


    if (args.length != 2) {
      System.out.println("dump <workloadname>");
      System.out.println("or");
      System.out.println("deploy <workloadfile>");
    }
    if (args[0].equals("deploy")) {
      applyWorkload(args[1]);
    }
    if (args[0].equals("dump")) {

      //for (Workload w : dl.getWorkloads()) {
      //  if (w.name.equals(args[1])) {
      //    dumpWorkload(w);
      //  }
      //}
    }
    workloadManager.shutdown();
  }

  public static void dumpWorkload(Workload w) {
    ObjectMapper map = new ObjectMapper();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      map.writeValue(baos, w);
    } catch (IOException ex) {
      System.out.println(ex);
    }
    System.out.println(baos.toString());
  }

  public static void applyWorkload(String file) {
    ObjectMapper m = new ObjectMapper();
    JavaType t = TypeFactory.type(Workload.class);
    Workload work = null;
    try {
      work = (Workload) m.readValue(new File(file), t);
    } catch (IOException ex) {
      System.out.println(ex);
    }
    if (work!=null){
       workloadManager.startWorkload(work);
    }
  }
}