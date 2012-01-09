package com.jointhegrid.ironcount;

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

  private IroncountWorkloadManager workloadManager;

  private static Cluster cluster;

  public static void main(String [] args) throws Exception {
    cluster = HFactory.createCluster("IroncountCluster",new CassandraHostConfigurator("localhost:9160"));
    // instantiate target (properties file for now)

    // TODO parse & apply parameters from CLI

    IroncountCli ic = new IroncountCli();

    ic.doExecute();

    ConsoleReader reader = new ConsoleReader();
    String line;
    while ((line = reader.readLine("[ironcount] ")) != null) {
      if ( line.equalsIgnoreCase("exit")) {
        System.exit(0);
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
      options.addOption("start", true, "Start Ironcount (default when no args are given)");
      options.addOption("stop", true, "Stop the Ironcount workers");
      return options;
  }


  private static CommandLine processArgs(String[] args, ConsoleReader reader) throws Exception {
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse( buildOptions(), args);
    if ( cmd.hasOption("help")) {
      HelpFormatter hf = new HelpFormatter();
      hf.printHelp( "ironcount [options]...", buildOptions() );
      return cmd;
    }
    return cmd;
  }

}
