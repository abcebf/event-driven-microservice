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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by Henry Huang on 7/30/16.
 */
@Component
public class ConsumerLauncher implements Runnable {
  @Autowired
  private ApplicationContext applicationContext;

  private ObjectMapper objectMapper;

  private KafkaStream<byte[], byte[]> kafkaStream;
  private ConsumerConnector consumer;
  private Method method;
  private Object controller;

  public ConsumerLauncher(KafkaStream<byte[], byte[]> kafkaStream, ConsumerConnector consumer,
                          Method method, Object controller) {
    this.kafkaStream = kafkaStream;
    this.consumer = consumer;

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
    ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();

    while (it.hasNext()) {
      byte[] messageData = it.next().message();

      try {
        Object videoFromMessage = objectMapper.readValue(messageData, method.getParameterTypes()[0]);
        try {
          //TODO: use ASM to enhance performance
          method.invoke(controller, videoFromMessage);
          //ensure that each event will be handled at least once
          consumer.commitOffsets(true);
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        } catch (InvocationTargetException e) {
          e.printStackTrace();
        }
      } catch (JsonParseException | JsonMappingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    System.out.println("Shutting down Thread: " + kafkaStream);
  }
}
