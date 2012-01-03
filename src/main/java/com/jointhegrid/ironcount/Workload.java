package com.jointhegrid.ironcount;

import java.util.Map;

public class Workload {

  public String name;
  public String topic;
  public String consumerGroup;
  public String messageHandlerName;
  public String zkConnect;
  public Integer maxWorkers;
  public Map<String, String> properties;
  public Boolean active;//want to use this

  public Workload() {
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Workload other = (Workload) obj;
    if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 79 * hash + (this.name != null ? this.name.hashCode() : 0);
    return hash;
  }

  @Override
  public String toString() {
    return "Workload{" + "name=" + name + "topic=" + topic + "consumerGroup=" + consumerGroup + "messageHandlerName=" + messageHandlerName + "maxWorkers=" + maxWorkers + "properties=" + properties + "active=" + active + '}';
  }
  
}