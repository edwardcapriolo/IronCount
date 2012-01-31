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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author edward
 */
public class SerializeWorkload {
  public static void main(String []args){
    Workload w = new Workload();
    w.active = true;
    w.consumerGroup = "group1";
    w.maxWorkers = 4;
    w.messageHandlerName = "com.jointhegrid.ironcount.mockingbird.MockingBirdMessageHandler";
    w.name = "testworkload";
    w.properties = new HashMap<String, String>();
    w.properties.put("aprop", "avalue");
    w.topic = "topic1";
    w.zkConnect = "localhost:8888";
    ObjectMapper map = new ObjectMapper();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      map.writeValue(baos, w);
    } catch (IOException ex) {
      System.out.println(ex);
    }
    System.out.println( baos.toString());

  }
}
