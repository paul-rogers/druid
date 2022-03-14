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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;

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
      @JsonProperty("containerPort") int containerPort,
      @JsonProperty("hostPort") int hostPort,
      @JsonProperty("startTimeoutSecs") int startTimeoutSecs
  )
  {
    super(service, containerPort, hostPort);
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

  public String resolveDockerHosts(String dockerHost)
  {
    // Handles just one host at present
    return resolveHosts(dockerHost, resolveHostPort());
  }

  public String resolveClusterHosts()
  {
    return resolveHosts(service(), resolveContainerPort());
  }

  private String resolveHosts(String host, int port)
  {
    return StringUtils.format("%s:%d", host, port);
  }

  @Override
  public String toString()
  {
    return TestConfigs.toYaml(this);
  }
}
