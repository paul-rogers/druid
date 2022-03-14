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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.ISE;

public class ServiceConfig
{
  private final String service;
  private final int containerPort;
  private final int hostPort;

  public ServiceConfig(
      String service,
      int containerPort,
      int hostPort
  )
  {
    this.service = service;
    this.containerPort = containerPort;
    this.hostPort = hostPort;
  }

  @JsonProperty("service")
  public String service()
  {
    return service;
  }

  @JsonProperty("containerPort")
  public int containerPort()
  {
    return containerPort;
  }

  @JsonProperty("hostPort")
  public int hostPort()
  {
    return hostPort;
  }

  public int resolveContainerPort()
  {
    if (containerPort != 0) {
      return containerPort;
    }
    if (hostPort == 0) {
      throw new ISE("Must provide containerPort, hostPort or both");
    }
    return hostPort;
  }

  public int resolveHostPort()
  {
    if (hostPort != 0) {
      return hostPort;
    }
    if (containerPort == 0) {
      throw new ISE("Must provide containerPort, hostPort or both");
    }
    return containerPort;
  }


}
