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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * YAML description of a ZK cluster. Converted to
 * {@link org.apache.druid.curator.CuratorConfig}
 */
public class ZKConfig extends ServiceConfig
{
  /**
   * Amount of time to wait for ZK to become ready.
   * Defaults to 5 seconds.
   */
  private final int startTimeoutSecs;

  @JsonCreator
  public ZKConfig(
      @JsonProperty("service") String service,
      @JsonProperty("startTimeoutSecs") int startTimeoutSecs,
      @JsonProperty("instances") List<ServiceInstance> instances
  )
  {
    super(service, instances);
    this.startTimeoutSecs = startTimeoutSecs;
  }

  //  public boolean validate(List<String> errs)
  //  {
  //    if (isEmpty(hosts) && isEmpty(exhibitors)) {
  //      errs.add("ZK: Provide either hosts or exhibitors");
  //      return false;
  //    }
  //    if (!isEmpty(hosts) || !isEmpty(exhibitors)) {
  //      errs.add("ZK: Provide only hosts or exhibitors, but not both");
  //      return false;
  //    }
  //    return true;
  //  }

  //  private static boolean isEmpty(List<String> list)
  //  {
  //    return list == null || list.isEmpty();
  //  }

  @JsonProperty("startTimeoutSecs")
  public int startTimeoutSecs()
  {
    return startTimeoutSecs;
  }

  @Override
  public String resolveService()
  {
    return service == null ? "zookeeper" : service;
  }

  public String resolveDockerHosts(String dockerHost)
  {
    List<String> hosts = new ArrayList<>();
    for (ServiceInstance instance : instances) {
      hosts.add(resolveHost(dockerHost, instance.resolveHostPort()));
    }
    return String.join(",", hosts);
  }

  public String resolveClusterHosts()
  {
    List<String> hosts = new ArrayList<>();
    for (ServiceInstance instance : instances) {
      hosts.add(resolveHost(instance.resolveContainerHost(resolveService()), instance.resolveContainerPort()));
    }
    return String.join(",", hosts);
  }

  private String resolveHost(String host, int port)
  {
    return StringUtils.format("%s:%d", host, port);
  }
}
