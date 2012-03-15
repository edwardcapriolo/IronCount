package com.jointhegrid.ironcount.manager;

public interface WorkerThreadMBean {
  public String getJSONSerializedWorkload();
  public void terminate();
  public long getMessagesProcessesed();
}
