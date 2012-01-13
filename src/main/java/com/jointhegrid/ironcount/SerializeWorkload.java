/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
