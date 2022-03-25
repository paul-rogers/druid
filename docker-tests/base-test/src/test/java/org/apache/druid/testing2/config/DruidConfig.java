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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a Druid service (of one or more instances) running
 * in the test cluster. The service name comes from the key used
 * in the {@code druid} map: <code><pre>
 * druid:
 *   broker:  # <-- key (service name)
 *     instances:
 *       ...
 * </pre></code>
 */
public class DruidConfig extends ServiceConfig
{
  private String serviceKey;

  @JsonCreator
  public DruidConfig(
      // Note: service is not actually used.
      @JsonProperty("service") String service,
      @JsonProperty("instances") List<ServiceInstance> instances
  )
  {
    super(service, instances);
  }

  protected void setServiceKey(String key)
  {
    if (serviceKey == null) {
      serviceKey = key;
    }
  }

  /**
   * Get the URL (visible to the test) of the service.
   */
  public String resolveUrl(String proxyHost)
  {
    return resolveUrl(proxyHost, instance());
  }

  /**
   * Find an instance given the instance name (tag).
   */
  public ServiceInstance findInstance(String instanceName)
  {
    for (ServiceInstance instance : requireInstances()) {
      if (instance.tag() != null && instance.tag().equals(instanceName)) {
        return instance;
      }
    }
    return null;
  }

  /**
   * Find an instance given the instance name (tag). Raises
   * an error (which fails the test) if the tag is not defined.
   */
  public ServiceInstance requireInstance(String instanceName)
  {
    ServiceInstance instance = findInstance(instanceName);
    if (instance != null) {
      return instance;
    }
    throw new ISE(
        StringUtils.format(
            "No Druid instance of service %s with name %s is defined",
            resolveService(),
            instanceName));
  }

  /**
   * Determines the name of the service from the key used to define
   * the service. An explicit service name takes precedence, but should
   * never be needed.
   */
  @Override
  public String resolveService()
  {
    String resolved = super.resolveService();
    return resolved == null ? serviceKey : resolved;
  }

  /**
   * Return the URL for the given instance name (tag) of this service
   * as visible to the test.
   */
  public String resolveUrl(String proxyHost, String instanceName)
  {
    return resolveUrl(proxyHost, requireInstance(instanceName));
  }

  /**
   * Return the host name, known to the cluster, for the given
   * service instance.
   */
  public String resolveHost(ServiceInstance instance)
  {
    return instance.resolveHost(resolveService());
  }

  /**
   * Return the URL, known to the test, of the given service instance.
   */
  public String resolveUrl(String proxyHost, ServiceInstance instance)
  {
    return StringUtils.format(
        "http://%s:%d",
        proxyHost,
        instance.resolveProxyPort());
  }

  /**
   * Return the named service instance. If not found, return the
   * "default" instance. This is used by the somewhat awkward test
   * config object so that if a test asks for "Coordinator one" in
   * a cluster with a single Coordinator, it will get that Coordinator.
   * Same for Overlord.
   */
  public ServiceInstance tagOrDefault(String tag)
  {
    ServiceInstance taggedInstance = findInstance(tag);
    return taggedInstance == null ? instance() : taggedInstance;
  }

  /**
   * Returns the "default" host for this service as known to the
   * cluster. The host is that of the only instance and is undefined
   * if there are multiple instances.
   */
  public String resolveHost()
  {
    ServiceInstance instance = instance();
    if (instances.size() > 1) {
      throw new ISE(
          StringUtils.format("Service %s has %d hosts, default is ambiguous",
              resolveService(),
              instances.size()));
    }
    return instance.resolveHost(resolveService());
  }

  public ServiceInstance findHost(String host)
  {
    Pattern p = Pattern.compile("https?://(.*):(\\d+)");
    Matcher m = p.matcher(host);
    if (!m.matches()) {
      return null;
    }
    String hostName = m.group(1);
    int port = Integer.parseInt(m.group(2));
    for (ServiceInstance instance : instances) {
      if (instance.resolveHost(resolveService()).equals(hostName) && instance.resolvePort() == port) {
        return instance;
      }
    }
    return null;
  }
}
