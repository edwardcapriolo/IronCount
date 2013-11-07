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
package com.jointhegrid.ironcount.mockingbird;

import com.jointhegrid.ironcount.manager.MessageHandler;
import com.jointhegrid.ironcount.manager.WorkerThread;
import com.jointhegrid.ironcount.manager.Workload;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Stack;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
/*
 * http://www.slideshare.net/kevinweil/rainbird-realtime-analytics-at-twitter-strata-2011
 *
 */

public class MockingBirdMessageHandler implements MessageHandler {

  private Workload w;
  Cluster cluster;
  Keyspace keyspace;
  DateFormat bucketByMinute = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

  public MockingBirdMessageHandler() {
  }

  @Override
  public void setWorkload(Workload w) {
    this.w = w;
    cluster = HFactory.getOrCreateCluster("mocking", w.properties.get("mocking.cas"));
    keyspace = HFactory.createKeyspace(w.properties.get("mocking.ks"), cluster);
  }

  @Override
  /* message here should be an url formatted as a string
  http://sub.domain.com/myurl.s becomes
  incr com by 1
  incr com/domain by 1
  incr com/domain/sub by 1
  incr com/domain/sub/myurl.s by 1
   */
  public void handleMessage(MessageAndMetadata m) {

    String url = m.message().toString();
    URI i = URI.create(url);
    String domain = i.getHost();
    String path = i.getPath();
    String[] parts = domain.split("\\.");
    Stack<String> s = new Stack<String>();
    s.add(path);
    s.addAll(Arrays.asList(parts));
    StringBuilder sb = new StringBuilder();

    for (int j = 0; j <= parts.length; j++) {
      sb.append(s.pop());
      countIt(sb.toString());
      sb.append(":");
    }
  }

  public void countIt(String s) {
    System.out.println("message into handler "+s);
    try {
      Mutator<String> m = HFactory.createMutator(keyspace, StringSerializer.get());
      HCounterColumn<String> hc = HFactory.createCounterColumn("count", 1L);
      m.addCounter(s + "/" + bucketByMinute.format(new Date()),
              w.properties.get("mocking.cf"), hc);
      m.execute();
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }

  @Override
  public void setWorkerThread(WorkerThread wt) {
  }

  @Override
  public void stop() {
  }

}
