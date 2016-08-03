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

import kafka.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * Created by Dongxiaojie on 7/30/16.
 */
@Configuration
@ComponentScan
public class ConsumerConfigFactory {
  @Value("${zookeeper.connect}")
  private String zookeeperConnection;
  @Value("${zookeeper.session.timeout.ms}")
  private String zookeeperSessionTimeoutMs;
  @Value("${zookeeper.sync.time.ms}")
  private String zookeeperSyncTimeMs;
  @Value("${auto.commit.interval.ms}")
  private String autoCommitIntervalMs;

  private Properties properties;

  @PostConstruct
  private void createConsumerConfig() {
    properties = new Properties();
    properties.put("zookeeper.connect", zookeeperConnection);
    properties.put("zookeeper.session.timeout.ms", zookeeperSessionTimeoutMs);
    properties.put("zookeeper.sync.time.ms", zookeeperSyncTimeMs);
    properties.put("auto.commit.interval.ms", autoCommitIntervalMs);
  }

  public ConsumerConfig getConsumerConfig(String groupId) {
    Properties props = new Properties();
    props.putAll(properties);
    props.put("group.id", groupId);
    return new ConsumerConfig(props);
  }
}