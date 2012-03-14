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
package com.jointhegrid.ironcount.user;

import com.jointhegrid.ironcount.manager.Workload;
import com.jointhegrid.ironcount.manager.WorkloadManager;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;

public class DeployTool {

  final static Logger logger = Logger.getLogger(ICLauncher.class.getName());
  private static Properties properties;
  private static WorkloadManager workloadManager;

  public static void main(String[] args) {

    properties = System.getProperties();
    if (properties.get(WorkloadManager.ZK_SERVER_LIST) == null) {
      logger.warn(WorkloadManager.ZK_SERVER_LIST+" was not defined setting to localhost:2181");
      properties.setProperty(WorkloadManager.ZK_SERVER_LIST, "localhost:2181");
    }
    //This should prevent te manager from picking up jobs
    properties.setProperty(WorkloadManager.IC_THREAD_POOL_SIZE, "0");

    workloadManager = new WorkloadManager(properties);
    workloadManager.init();


    if (args.length != 2) {
      System.out.println("dump <workloadname>");
      System.out.println("or");
      System.out.println("deploy <workloadfile>");
    }
    if (args[0].equals("deploy")) {
      applyWorkload(args[1]);
    } else if (args[0].equals("dump")) {
      for (Workload w : workloadManager.getAllWorkloads()){
        if (w.name.equals(args[1])) {
          dumpWorkload(w);
        }
      }
    }
    workloadManager.shutdown();
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

  public static void applyWorkload(String file) {
    ObjectMapper m = new ObjectMapper();
    JavaType t = TypeFactory.type(Workload.class);
    Workload work = null;
    try {
      work = (Workload) m.readValue(new File(file), t);
    } catch (IOException ex) {
      System.out.println(ex);
    }
    if (work!=null){
       workloadManager.startWorkload(work);
    }
  }
}