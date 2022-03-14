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

package org.apache.druid.testing.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.druid.java.util.common.ISE;

import java.io.IOException;
import java.io.InputStream;

public class TestConfigs
{
  public static String toYaml(Object obj)
  {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      return "<conversion failed>";
    }
  }

  public static ClusterConfig loadFromResource(String resource)
  {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try (InputStream is = TestConfigs.class.getResourceAsStream(resource)) {
      if (is == null) {
        throw new ISE("Config resoure not found: " + resource);
      }
      return mapper.readValue(is, ClusterConfig.class);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to load config resoure: " + resource);
    }
  }
}
