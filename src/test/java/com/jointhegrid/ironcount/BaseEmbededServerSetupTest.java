package com.jointhegrid.ironcount;

import java.io.IOException;

import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
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
  }

}
