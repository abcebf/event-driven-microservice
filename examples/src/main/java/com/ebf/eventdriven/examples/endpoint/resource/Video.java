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

package com.ebf.eventdriven.examples.endpoint.resource;

/**
 * Created by henry huang on 7/30/16.
 */
public class Video {

  private static final int IMPOSSIBLE_ID = -1;

  private int id = IMPOSSIBLE_ID;
  private String identifier;
  private String provider;
  private String title;
  private String description;

  public int getId() {
    return id;
  }

  public String getIdentifier() {
    return identifier;
  }

  public String getProvider() {
    return provider;
  }

  public String getTitle() {
    return title;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return "Video [identifier=" + identifier + ", provider=" + provider
        + ", title=" + title + "]";
  }

}
