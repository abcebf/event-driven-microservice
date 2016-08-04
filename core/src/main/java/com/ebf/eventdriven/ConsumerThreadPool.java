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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Henry Huang on 7/30/16.
 */
@Component
public class ConsumerThreadPool {
  @Value("${thread.number}")
  private final Integer threadNum = 1;

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private ConsumerConfigFactory consumerConfigFactory;

  private ExecutorService threadPool;

  public ConsumerThreadPool() {
    threadPool = Executors.newFixedThreadPool(threadNum);
  }

  @PostConstruct
  public void startConsuming() {
    loadConsumers();
  }

  private void loadConsumers() {
    Map<String, Object> controllers = applicationContext.getBeansWithAnnotation(ConsumerController.class);
    for (String controllerName : controllers.keySet()) {
      Object controller = controllers.get(controllerName);
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
    threadPool.submit(applicationContext.getBean(ConsumerLauncher.class, topic, group, method, controller));
  }
}
