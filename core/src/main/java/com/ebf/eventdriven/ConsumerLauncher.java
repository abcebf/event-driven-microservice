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
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by Dongxiaojie on 7/30/16.
 */
public class ConsumerLauncher implements Runnable {
  private ObjectMapper objectMapper;
  private KafkaStream<byte[], byte[]> kafkaStream;
  private int threadNumber;
  private Method method;
  private Object controller;

  public ConsumerLauncher(KafkaStream<byte[], byte[]> kafkaStream, int threadNumber,
                          Method method, Object controller) {
    this.threadNumber = threadNumber;
    this.kafkaStream = kafkaStream;
    this.objectMapper = new ObjectMapper();
    this.method = method;
    this.controller = controller;
  }

  @Override
  public void run() {
    ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();

    while (it.hasNext()) {
      byte[] messageData = it.next().message();
      try {
        Object videoFromMessage = objectMapper.readValue(messageData, method.getParameterTypes()[0]);
        try {
          method.invoke(controller, videoFromMessage);
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
