package com.jointhegrid.ironcount;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;

public class DataLayer {

  public static final String IRONCOUNT_KEYSPACE = "ironcount";
  public static final String WORKLOAD_CF = "workload";
  public static final String JOBINFO_CF = "jobinfo";
  public Cluster cluster;
  public Keyspace keyspace;

  public ColumnFamilyTemplate<String,String> workloadTemplate;
  public ColumnFamilyTemplate<String,String> jobInfoTemplate;

  public DataLayer(Cluster cluster) {
    this.cluster = cluster;
    keyspace = HFactory.createKeyspace(IRONCOUNT_KEYSPACE, cluster);
    workloadTemplate =
            new ThriftColumnFamilyTemplate<String, String>(keyspace,
            WORKLOAD_CF,
            StringSerializer.get(),
            StringSerializer.get());

    jobInfoTemplate =
            new ThriftColumnFamilyTemplate<String, String>(keyspace,
            JOBINFO_CF,
            StringSerializer.get(),
            StringSerializer.get());
  }

  public void createMetaInfo() {
    KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(IRONCOUNT_KEYSPACE);
    System.err.println("createMetaInfo");
    if (keyspaceDef == null) {
      try {
      KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition(IRONCOUNT_KEYSPACE);
      ColumnFamilyDefinition cfwork = HFactory.createColumnFamilyDefinition(IRONCOUNT_KEYSPACE, WORKLOAD_CF, ComparatorType.UTF8TYPE);
      ColumnFamilyDefinition cfjob = HFactory.createColumnFamilyDefinition(IRONCOUNT_KEYSPACE, JOBINFO_CF, ComparatorType.UTF8TYPE);
      cluster.addKeyspace(ksDef,true);
      cluster.addColumnFamily(cfwork,true);
      cluster.addColumnFamily(cfjob,true);
      } catch (Exception ex){ throw new RuntimeException(ex);}
    }
  }

  public List<Workload> getWorkloads() {

    List<Workload> ret = new ArrayList<Workload>();
    ColumnFamilyResult<String, String> workload =
            workloadTemplate.queryColumns("workload");
    Collection<String> jobNames = workload.getColumnNames();
    for (String job : jobNames) {
      String workS = workload.getString(job);
      ObjectMapper m = new ObjectMapper();
      JavaType t = TypeFactory.type(Workload.class);
      Workload work = null;
      try {
        work = (Workload) m.readValue(workS, t);
        ret.add(work);
      } catch (IOException ex) {
        System.out.println(ex);
      }
    }
    return ret;
  }

  public JobInfo getJobInfoForWorkload(Workload w){
    ColumnFamilyResult<String,String> res = jobInfoTemplate.queryColumns(w.name);
    JobInfo ji = new JobInfo();
    ji.workloadName=w.name;
    ji.workerIds=res.getColumnNames();
    return ji;
  }

  public void registerJob(Workload w,WorkerThread wt){
    ColumnFamilyUpdater<String,String> ji = jobInfoTemplate.createUpdater(w.name);
    ji.setString(wt.wtId.toString(), "");
    jobInfoTemplate.update(ji);
  }

  public void startWorkload(Workload w) {
    ColumnFamilyUpdater<String, String> up = workloadTemplate.createUpdater("workload");
    ObjectMapper map = new ObjectMapper();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      map.writeValue(baos, w);
    } catch (IOException ex) {
      System.out.println(ex);
    }
    up.setString(w.name, baos.toString());
    workloadTemplate.update(up);
  }
}
