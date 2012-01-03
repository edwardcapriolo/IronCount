package com.jointhegrid.ironcount;

import java.util.Collection;

public class JobInfo {
  public String workloadName;//row key
  public Collection<String> workerIds; //id of the server doing the job
  public JobInfo(){

  }
}
