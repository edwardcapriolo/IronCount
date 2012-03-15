/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jointhegrid.ironcount.manager;

import java.util.List;

/**
 *
 * @author edward
 */
public interface WorkloadManagerMBean {
  public void setRescanMillis(long millis);
  public long getRescanMillis();

  public void setThreadPoolSize(int size);
  public int getThreadPoolSize();

  public void applyWorkload(String workloadAsJson);
  public List<String> getConfiguredWorkloadNames();
  public String getWorkloadAsJSON(String workloadName);
}
