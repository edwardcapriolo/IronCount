package com.jointhegrid.ironcount;

import java.util.Properties;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.log4j.Logger;

public class ICLauncher {

  final static Logger logger = Logger.getLogger(ICLauncher.class.getName());
  private static IroncountWorkloadManager workloadManager;
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
    KeyspaceDefinition kd = cluster.describeKeyspace(DataLayer.IRONCOUNT_KEYSPACE);
    if (kd == null){
      logger.warn("keyspace does not exist will create.");
      dl.createMetaInfo();
    }

    workloadManager = new IroncountWorkloadManager(cluster);
    workloadManager.init();
    workloadManager.execute();
  }
}