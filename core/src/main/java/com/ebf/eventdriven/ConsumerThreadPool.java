/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ebf.eventdriven;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static kafka.consumer.Consumer.createJavaConsumerConnector;

/**
 * Created by Dongxiaojie on 7/30/16.
 */
@Component
public class ConsumerThreadPool {
  private static final Integer NUM_THREADS = 1;

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private ConsumerConfigFactory consumerConfigFactory;

  private ExecutorService threadPool;

  public ConsumerThreadPool() {
    threadPool = Executors.newFixedThreadPool(NUM_THREADS);
  }

  @PostConstruct
  public void startConsuming() {
    loadConsumers();
  }

  private void loadConsumers() {
    Map<String, Object> controllers = applicationContext.getBeansWithAnnotation(ConsumerController.class);
    for (Object controller : controllers.values()) {
      Class<?> clazz = controller.getClass();
      Annotation[] annotations = clazz.getAnnotations();
      for (Annotation annotation : annotations) {
        if (annotation instanceof ConsumerController) {
          Method[] methods = clazz.getMethods();
          for (Method method : methods) {
            Topic topicDesc = method.getAnnotation(Topic.class);
            if (topicDesc == null) {
              continue;
            }
            String group = topicDesc.group();
            if (topicDesc.dynamicGroup()) {
              try {
                group = group + InetAddress.getLocalHost().getHostAddress();
              } catch (UnknownHostException e) {
                group = group + UUID.randomUUID().toString();
              }
            }
            consume(topicDesc.value(), group, method, controller);
          }
          break;
        }
      }
    }
  }

  private void consume(String topic, String group, Method method, Object controller) {
    ConsumerConnector consumer = createJavaConsumerConnector(consumerConfigFactory.getConsumerConfig(group));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, NUM_THREADS);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

    int threadNumber = 0;
    for (final KafkaStream<byte[], byte[]> stream : streams) {
      threadPool.submit(new ConsumerLauncher(stream, threadNumber, method, controller));
      threadNumber++;
    }
  }
}
