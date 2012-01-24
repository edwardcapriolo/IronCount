package com.jointhegrid.ironcount;

import java.util.Properties;
import org.apache.log4j.Logger;

public class ICLauncher {

  final static Logger logger = Logger.getLogger(ICLauncher.class.getName());
  private static WorkloadManager workloadManager;
  private static Properties properties;
  
  public static void main(String[] args) {
    properties = System.getProperties();
    if (properties.get(WorkloadManager.ZK_SERVER_LIST) == null) {
      logger.warn(WorkloadManager.ZK_SERVER_LIST+" was not defined setting to localhost:2181");
      properties.setProperty(WorkloadManager.ZK_SERVER_LIST, "localhost:2181");
    }

    workloadManager = new WorkloadManager(properties);
    workloadManager.init();
  }
}