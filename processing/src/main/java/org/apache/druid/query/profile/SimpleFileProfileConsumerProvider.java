/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.profile;

import java.io.File;

import org.apache.druid.query.profile.ProfileConsumer.ProfileConsumerStub;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(SimpleFileProfileConsumerProvider.TYPE_NAME)
public class SimpleFileProfileConsumerProvider implements ProfileConsumerProvider
{
  public static final String TYPE_NAME = "simple";
  public static final String DEFAULT_DIR = "/tmp/druid/profiles";
  
  private ProfileConsumer instance;
  
  /**
   * Location to put the profiles. The default is for tests, and
   * in case someone enables the property without proper config.
   * The default config files should use var/druid/profiles, where
   * "var" is relative to the Druid install directory.
   */
  @JsonProperty
  private String dir = DEFAULT_DIR;

  @Override
  public ProfileConsumer get()
  {
    if (instance == null) {
      File profileDir = new File(dir).getAbsoluteFile();
      profileDir.mkdirs();
      if (profileDir.exists() && profileDir.canWrite()) {
        instance = new ProfileWriter(profileDir);
      } else {
        instance = new ProfileConsumerStub();
      }
    }
    return instance;
  }
}
