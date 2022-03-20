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

package org.apache.druid.test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.druid.testing2.config.ClusterConfig;
import org.junit.Test;

import java.io.IOException;

/**
 * Sanity check of an example YAML config file using the Java
 * deserialization classes.
 */
public class TestClusterConfig
{
  @Test
  public void testYaml() throws JsonParseException, JsonMappingException, IOException
  {
    ClusterConfig obj = ClusterConfig.loadFromResource("/yaml/test.yaml");
    System.out.println(obj);
  }
}
