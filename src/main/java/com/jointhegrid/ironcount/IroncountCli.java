package com.jointhegrid.ironcount;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import jline.ConsoleReader;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.commons.cli.*;

/**
 * Entry point for controlling IronCount service
 * @author zznate
 */
public class IroncountCli {

  private static IroncountWorkloadManager workloadManager;
  private static Properties properties;
  private static Cluster cluster;

  public static void main(String [] args) throws Exception {
    cluster = HFactory.createCluster("IroncountCluster",new CassandraHostConfigurator("localhost:9160"));
    // instantiate target (properties file for now)

    // TODO parse & apply parameters from CLI
    // TODO push through config properties file
    IroncountCli ic = new IroncountCli();

    ic.doExecute();

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
    workloadManager = new IroncountWorkloadManager(cluster);
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
