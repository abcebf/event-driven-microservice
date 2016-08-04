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

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Henry Huang on 7/30/16.
 */
@Component
public class ConsumerLauncher implements Runnable {
  @Autowired
  private ApplicationContext applicationContext;
  @Autowired
  private ConsumerConfigFactory consumerConfigFactory;

  private ObjectMapper objectMapper;

  private String topic;
  private String  group;
  private Method method;
  private Object controller;

  public ConsumerLauncher(String topic, String group,
                          Method method, Object controller) {
    this.topic = topic;
    this.group = group;

    //TODO: use spring container to create an instance of ObjectMapper
    ObjectMapper objMapper = null;

    try {
      objMapper = applicationContext.getBean(ObjectMapper.class);
    } catch(Exception ex) {
    }

    if (objMapper == null) {
      objMapper = new ObjectMapper();
    }
    this.objectMapper = objMapper;
    this.method = method;
    this.controller = controller;
    //this.controller = applicationContext.getBean(controllerName);
  }

  @Override
  public void run() {
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    int count = 0;
    KafkaConsumer<String, String> consumer = null;

    try {
      consumer =
          new KafkaConsumer<String, String>(consumerConfigFactory.getConsumerConfig(group),
              new StringDeserializer(),
              new StringDeserializer()
          );

      final KafkaConsumer<String, String> theConsumer = consumer;
      final Thread mainThread = Thread.currentThread();

      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          System.out.println("Starting exit...");
          // Note that shutdownhook runs in a separate thread,
          // so the only thing we can safely do to a consumer is wake it up
          theConsumer.wakeup();
          try {
            mainThread.join();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });

      consumer.subscribe(Collections.singletonList(topic),
          new EbfConsumerRebalanceListener(consumer, currentOffsets));

      boolean shouldRun = true;
      while (shouldRun) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        Map<String, EbfEvent> failHandledEvents = new HashMap<>();
        for (ConsumerRecord<String, String> record : records)
        {
          System.out.println(String.format("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value()));
          currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset()));

          EbfEvent ebfEvent = null;
          try {
            if (method.getGenericParameterTypes().length != 1) {
              throw new RuntimeException("invalid handler signature.");
            }
            Type eventType = method.getGenericParameterTypes()[0];

            if (eventType instanceof ParameterizedType) {
              Type[] parameters = ((ParameterizedType)eventType).getActualTypeArguments();
              Class<?>[] contentTypes = new Class<?>[parameters.length];
              for (int i=0; i<parameters.length; i++) {
                contentTypes[i] = (Class<?>)parameters[i];
              }
              JavaType type = objectMapper.getTypeFactory().constructParametricType(method.getParameterTypes()[0], contentTypes);
              ebfEvent = objectMapper.readValue(record.value(), type);
            } else {
              throw new RuntimeException("invalid handler signature.");
              //event = objectMapper.readValue(record.value(), method.getParameterTypes()[0]);
            }

            if (failHandledEvents.containsKey(ebfEvent.getProducerLogNo())) {
              if (ebfEvent.isLast()) {
                failHandledEvents.remove(ebfEvent.getProducerLogNo());
              }
            } else {
              try {
                //TODO: use ASM to enhance performance
                method.invoke(controller, ebfEvent);
                //ensure that each event will be handled at least once
              } catch (IllegalAccessException e) {
                //TODO
                e.printStackTrace();
                shouldRun = false;
                break;
              } catch (InvocationTargetException e) {
                //TODO
                e.printStackTrace();
                shouldRun = false;
                break;
              }
            }
          } catch (Exception ex) {
            failHandledEvents.put(ebfEvent.getProducerLogNo(), ebfEvent);
          }

          if (count % 1000 == 0) {
            consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
              public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                  //commit failed
                }
              }
            });
          }
          count++;
        }
      }
    } catch (Exception e) {

    } finally {
      if (consumer != null) {
        try {
          consumer.commitSync();
        } finally {
          consumer.close();
          consumer = null;
        }
      }
    }
  }
}
