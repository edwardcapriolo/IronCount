package com.jointhegrid.ironcount;

import java.io.IOException;

import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.testutils.EmbeddedServerHelper;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;

/**
 * Base class for test cases that need access to EmbeddedServerHelper
 *
 * @author zznate
 *
 */
public abstract class BaseEmbededServerSetupTest {

  private static EmbeddedServerHelper embedded;

  protected static HConnectionManager connectionManager;
  protected static CassandraHostConfigurator cassandraHostConfigurator;
  protected static String clusterName = "TestCluster";
  protected static Cluster cluster;

  /**
   * Set embedded cassandra up and spawn it in a new thread.
   *
   * @throws org.apache.thrift.transport.TTransportException
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @BeforeClass
  public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException {
    if ( embedded == null ) {
      embedded = new EmbeddedServerHelper();
      embedded.setup();
    }

    cassandraHostConfigurator = new CassandraHostConfigurator("localhost:9157");
    cluster = HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator);

    buildTestSchema();
  }

  private static void buildTestSchema() {
    // schema creation as that is handled in DataLayer
    // TODO encapsulate this in a separate util class
    KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(DataLayer.IRONCOUNT_KEYSPACE);

    if (keyspaceDef == null) {
      try {
      KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition(DataLayer.IRONCOUNT_KEYSPACE);
      ColumnFamilyDefinition cfwork = HFactory.createColumnFamilyDefinition(DataLayer.IRONCOUNT_KEYSPACE, DataLayer.WORKLOAD_CF, ComparatorType.UTF8TYPE);
      ColumnFamilyDefinition cfjob = HFactory.createColumnFamilyDefinition(DataLayer.IRONCOUNT_KEYSPACE, DataLayer.JOBINFO_CF, ComparatorType.UTF8TYPE);
      cluster.addKeyspace(ksDef,true);
      cluster.addColumnFamily(cfwork,true);
      cluster.addColumnFamily(cfjob,true);
      } catch (Exception ex){ throw new RuntimeException(ex);}
    }
  }

}
