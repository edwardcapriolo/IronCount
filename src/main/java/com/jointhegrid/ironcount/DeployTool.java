package com.jointhegrid.ironcount;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;

public class DeployTool {

  final static Logger logger = Logger.getLogger(ICLauncher.class.getName());
  private static Properties properties;
  private static Cluster cluster;

  public static void main(String[] args) {

    properties = System.getProperties();
    if (properties.get("cassandra.hosts") == null) {
      logger.warn("cassandra.hosts was not defined setting to localhost:9160");
      properties.setProperty("cassandra.hosts", "localhost:9160");
    }
    cluster = HFactory.getOrCreateCluster("IroncountCluster",
            new CassandraHostConfigurator(properties.getProperty("cassandra.hosts")));

    DataLayer dl = new DataLayer(cluster);
    if (args.length != 2) {
      System.out.println("dump <workloadname>");
      System.out.println("or");
      System.out.println("deploy <workloadfile>");
    }
    if (args[0].equals("deploy")) {
      applyWorkload(args[1],dl);
    }
    if (args[0].equals("dump")) {
      for (Workload w : dl.getWorkloads()) {
        if (w.name.equals(args[1])) {
          dumpWorkload(w);
        }
      }
    }
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

  public static void applyWorkload(String file, DataLayer dl) {
    ObjectMapper m = new ObjectMapper();
    JavaType t = TypeFactory.type(Workload.class);
    Workload work = null;
    try {
      work = (Workload) m.readValue(new File(file), t);
    } catch (IOException ex) {
      System.out.println(ex);
    }
    if (work!=null){
      dl.startWorkload(work);
    }
  }
}
