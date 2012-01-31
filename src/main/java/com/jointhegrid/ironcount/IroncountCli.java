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

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import jline.ConsoleReader;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

/**
 * Entry point for controlling IronCount service
 * @author zznate
 */
public class IroncountCli {

  final static Logger logger = Logger.getLogger(IroncountCli.class.getName());

  private static WorkloadManager workloadManager;
  private static Properties properties;
  private static Cluster cluster;

  public static void main(String [] args) throws Exception {
    properties = System.getProperties();
    if (properties.get("cassandra.hosts") == null){
      logger.warn("cassandra.hosts was not defined setting to localhost:9160");
      properties.setProperty("cassandra.hosts", "localhost:9160");
    }
    cluster = HFactory.createCluster("IroncountCluster",
            new CassandraHostConfigurator(properties.getProperty("cassandra.hosts")));
    
    // TODO parse & apply parameters from CLI
    // TODO push through config properties file
    IroncountCli ic = new IroncountCli();

    ic.doExecute();

    logger.warn("starting console reader");
    ConsoleReader reader = new ConsoleReader();

    String line;
    while ((line = reader.readLine("[ironcount] ")) != null) {
      if ( line.equalsIgnoreCase("exit") ) {
        System.exit(0);
      } else if ( line.equalsIgnoreCase("stop") ) {
      reader.printString("Stopping ironcount workload manager...");
      workloadManager.shutdown();
      reader.printString("Stopped. \n");
    } else if ( line.equalsIgnoreCase("start") ) {
      reader.printString("Starting ironcount workload manager...");
      workloadManager.init();
      reader.printString("OK \n");
    }
      processArgs(line.split(" "), reader);
    }
  }

  private void doExecute() {
    // initialize IroncountWorkloadManager
 //   workloadManager = new IroncountWorkloadManager(cluster);
    workloadManager.init();
    // produce output
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this help message and exit");
    options.addOption("p","props", true, "The properties file from which we will load ironcount settings");
    return options;
  }


  private static CommandLine processArgs(String[] args, ConsoleReader reader) throws Exception {
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse( buildOptions(), args);
    if ( cmd.hasOption("help")) {
      HelpFormatter hf = new HelpFormatter();
      hf.printHelp("ironcount [options]...", buildOptions());
      return cmd;
    }
    // TODO differentiate between startup vs. running commands
    if ( cmd.hasOption("props") ) {
      properties = new Properties();
      properties.load(new FileReader(new File(cmd.getOptionValue("props"))));
    }

    return cmd;
  }

}
