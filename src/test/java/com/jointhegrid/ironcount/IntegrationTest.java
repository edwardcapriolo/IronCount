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


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import org.junit.Test;

public class IntegrationTest extends IronIntegrationTest {

  public static final String EVENTS = "events";
  @Test
  public void hello() throws InterruptedException {
   
    createTopic(EVENTS, 1, 1);
    Producer<String, String> producer = new Producer<String, String>(super.createProducerConfig());
    ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(super.createConsumerConfig());

    for (int i = 0; i < 2000; i++) {
      producer.send(new KeyedMessage<String, String>(EVENTS, "1", i + ""));
    }

    Map<String, Integer> consumers = new HashMap<String, Integer>();
    consumers.put(EVENTS, 1);
    StringDecoder decoder = new StringDecoder(new VerifiableProperties());
    Map<String, List<KafkaStream<String, String>>> topicMessageStreams = consumerConnector
            .createMessageStreams(consumers, decoder, decoder);

    final List<KafkaStream<String, String>> streams = topicMessageStreams.get(EVENTS);
    final AtomicInteger in = new AtomicInteger();
    final KafkaStream<String, String> stream = streams.get(0);

    Thread t = new Thread() {

      public void run() {
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext()){
          String msg = it.next().message();
          System.out.println(msg);
          in.incrementAndGet();
          if (in.get() == 1999) {
            break;
          }
        }
      }
    };

    t.start();
    t.join();

    Assert.assertEquals(1999, in.get());

  }

}
