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

import com.jointhegrid.ironcount.manager.WorkloadManager;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ICLauncher {

  final static Logger logger = Logger.getLogger(ICLauncher.class.getName());
  private static WorkloadManager workloadManager;
  private static Properties properties;
  
  public static void main(String[] args) {
    properties = System.getProperties();
    if (properties.get(WorkloadManager.ZK_SERVER_LIST) == null) {
      logger.warn(WorkloadManager.ZK_SERVER_LIST+" was not defined. Defaulting to localhost:2181");
      properties.setProperty(WorkloadManager.ZK_SERVER_LIST, "localhost:2181");
    }

    workloadManager = new WorkloadManager(properties);
    workloadManager.init();
  }
}