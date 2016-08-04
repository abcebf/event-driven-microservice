/**
 * See the LICENSE file distributed with
 * this work for additional information regarding copyright ownership.
 * This file is licensed to You under the Apache License, Version 2.0
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

package com.ebf.eventdriven.examples.endpoint.controller;

import com.ebf.eventdriven.ConsumerController;
import com.ebf.eventdriven.EbfEvent;
import com.ebf.eventdriven.Topic;
import com.ebf.eventdriven.examples.endpoint.resource.Video;

/**
 * Created by hhuang on 7/29/16.
 */
@ConsumerController
public class VideoController {

  @Topic(value = "video_test", dynamicGroup = true)
  public void consumeVideoMessage(EbfEvent<Video> videoEvent) {
    System.out.println("#####consuming video######");
    System.out.println(String.format("id=%1s, title=%2s", videoEvent.getBody().getId(), videoEvent.getBody().getTitle()));
  }
}
