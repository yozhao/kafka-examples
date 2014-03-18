/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

public class Consumer extends Thread {
  private ConsumerConnector consumer;
  private final String topic;

  public Consumer(String topic) {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    this.topic = topic;
  }

  private static ConsumerConfig createConsumerConfig() {
    Properties props = new Properties();
    // zk connection
    props.put("zk.connect", "localhost");
    // Same group id means consume message separately
    props.put("groupid", 0);
    // The zookeeper session timeout.
    props.put("zk.sessiontimeout.ms", "400");
    // Max time for how far a ZK follower can be behind a ZK leader
    props.put("zk.synctime.ms", "200");
    // The time interval at which to save the current offset in ms
    props.put("autocommit.interval.ms", "1000");
    // Consumer timeout, if not set consumer will wait forever
    props.put("consumer.timeout.ms", "5000");
    return new ConsumerConfig(props);
  }

  private static String getMessage(Message message) {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }

  @Override
  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    // get all message in one stream
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<Message>>> consumerMap = consumer
        .createMessageStreams(topicCountMap);
    KafkaStream<Message> stream = consumerMap.get(topic).get(0);
    ConsumerIterator<Message> it = stream.iterator();
    Map<String, Long> partitionOffsetMap = new TreeMap<String, Long>();
    while (true) {
      try {
        if (!it.hasNext()) {
          break;
        }
      } catch (Exception e) {
        // Most likely timeout exception
        break;
      }
      MessageAndMetadata<Message> messageAndMetadata = it.next();
      System.out.println(getMessage(messageAndMetadata.message()));
      partitionOffsetMap.put(messageAndMetadata.partition(), messageAndMetadata.offset());
    }
    consumer.shutdown();

    System.out.println("Seek to start point...");
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    for (Map.Entry<String, Long> entry : partitionOffsetMap.entrySet()) {
      partitionOffsetMap.put(entry.getKey(), 0l);
    }
    // Rewrite offset in zk
    consumer.commitOffsets(topic, partitionOffsetMap);
    consumerMap = consumer.createMessageStreams(topicCountMap);
    stream = consumerMap.get(topic).get(0);
    it = stream.iterator();
    while (true) {
      try {
        if (!it.hasNext()) {
          break;
        }
      } catch (Exception e) {
        // Most likely timeout exception
        break;
      }
      MessageAndMetadata<Message> messageAndMetadata = it.next();
      System.out.println(getMessage(messageAndMetadata.message()));
    }
    consumer.shutdown();
    System.out.println("Consumer thread exit.");
  }
}
