
/*
Copyright 2011 Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
