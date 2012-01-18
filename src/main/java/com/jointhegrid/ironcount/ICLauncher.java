package com.jointhegrid.ironcount;

import java.util.Properties;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.log4j.Logger;

public class ICLauncher {

  final static Logger logger = Logger.getLogger(ICLauncher.class.getName());
  private static WorkloadManager workloadManager;
  private static Properties properties;
  

  public static void main(String[] args) {
    properties = System.getProperties();
    if (properties.get("zk.hosts") == null) {
      logger.warn("zk.hosts was not defined setting to localhost:9160");
      properties.setProperty("zkhosts", "localhost:2181");
    }

    workloadManager = new WorkloadManager(properties);
    workloadManager.init();
    //workloadManager.execute();
  }
}