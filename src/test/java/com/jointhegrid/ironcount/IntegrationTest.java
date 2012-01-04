package com.jointhegrid.ironcount;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import org.junit.Test;

public class IntegrationTest extends IronIntegrationTest {


  @Test
  public void hello() {
    producer.send(new ProducerData<Integer, String>(topic, "1 b c"));
    producer.send(new ProducerData<Integer, String>(topic, "d e f"));

    Map<String, Integer> consumers = new HashMap<String, Integer>();
    consumers.put(this.topic, 1);
    Map<String, List<KafkaMessageStream<Message>>> topicMessageStreams =
            consumerConnector.createMessageStreams(consumers);
    List<KafkaMessageStream<Message>> streams = topicMessageStreams.get(this.topic);

    int x=0;
    // consume the messages in the threads
    for (KafkaMessageStream<Message> stream : streams) {
      for (Message message : stream) {
        System.out.println(getMessage(message));
        x++;
        if (x==2){
          break;
        }
      }
    }
    
  }

  public static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }
}
