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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import org.junit.Test;


public class IntegrationTest extends IronIntegrationTest {


  @Test
  public void hello() throws InterruptedException {
    for (int i =0;i<2000;i++){
      producer.send(new KeyedMessage<String, String>(EVENTS,"1", "1 b c"));
      producer.send(new KeyedMessage<String, String>(EVENTS,"1", "d e f"));
    }
    
    producer.close();
    Map<String, Integer> consumers = new HashMap<String, Integer>();
    consumers.put(this.EVENTS, 1);
    StringDecoder decoder =
            new StringDecoder(new VerifiableProperties());
    Map<String, List<KafkaStream<String, String>>> topicMessageStreams =
            consumerConnector.createMessageStreams(consumers,decoder,decoder);
    
    final List<KafkaStream<String, String>> streams = topicMessageStreams.get(this.EVENTS);
    System.out.println("Streams size"+ streams.size());
    
    // consume the messages in the threads
    System.out.println("Starting consumers");
   
    
    final AtomicInteger in = new AtomicInteger();
    
    
    final ConsumerIterator i = streams.get(0).iterator();
    
    
    Thread kafkaMessageReceiverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        
       try {
        while (i.hasNext() && in.get() <= 2000) {
          String msg = i.next().message().toString();
          System.out.println(msg);
          System.out.println(in.get());
          in.incrementAndGet();
        }
       } catch (Exception ex){
         ex.printStackTrace();
       }
      }
    }, "kafkaMessageReceiverThread");
    kafkaMessageReceiverThread.start();
    kafkaMessageReceiverThread.join();
    Assert.assertEquals(20000, in.get());
    
  }
/*
  public static String getMessage(MessageAndMetadata<Message> message) {
    ByteBuffer buffer = message.message().payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
  */
}
