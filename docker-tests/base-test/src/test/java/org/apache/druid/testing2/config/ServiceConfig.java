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

package org.apache.druid.testing2.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.ISE;

import java.util.List;

public class ServiceConfig
{
  protected final String service;
  protected List<ServiceInstance> instances;

  public ServiceConfig(
      String service,
      List<ServiceInstance> instances
  )
  {
    this.service = service;
    this.instances = instances;
  }

  @JsonProperty("service")
  @JsonInclude(Include.NON_NULL)
  public String service()
  {
    return service;
  }

  @JsonProperty("instances")
  @JsonInclude(Include.NON_NULL)
  public List<ServiceInstance> instances()
  {
    return instances;
  }

  public String resolveService()
  {
    return service;
  }

  protected List<ServiceInstance> requireInstances()
  {
    if (instances == null || instances.isEmpty()) {
      throw new ISE("Please specify a " + resolveService() + " instance");
    }
    return instances;
  }

  protected ServiceInstance instance()
  {
    return requireInstances().get(0);
  }

  @Override
  public String toString()
  {
    return TestConfigs.toYaml(this);
  }
}
