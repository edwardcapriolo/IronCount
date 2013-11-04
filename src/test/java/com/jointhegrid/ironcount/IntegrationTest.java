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

import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;

import org.junit.Test;


public class IntegrationTest extends IronIntegrationTest {


  @Test
  public void hello() {
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "1 b c"));
    producer.send(new KeyedMessage<Integer, String>(EVENTS, "d e f"));

    Map<String, Integer> consumers = new HashMap<String, Integer>();
    consumers.put(this.EVENTS, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams =
            consumerConnector.createMessageStreams(consumers);
    List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get(this.EVENTS);

    int x=0;
    // consume the messages in the threads
    for (KafkaStream<byte[], byte[]> stream : streams) {
      for (MessageAndMetadata<byte[], byte[]> message : stream) {
       
        x++;
        if (x==2){
          break;
        }
      }
    }
    
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
