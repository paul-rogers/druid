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

package org.apache.druid.testing2.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing2.config.ClusterConfig;
import org.apache.druid.testing2.config.DruidConfig;
import org.apache.druid.testing2.config.ServiceInstance;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Client to the Druid cluster described by the test cluster
 * configuration.
 */
public class ClusterClient
{
  private final ClusterConfig config;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public ClusterClient(
      ClusterConfig config,
      @TestClient HttpClient httpClient,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
  }

  public ClusterConfig config()
  {
    return config;
  }

  /**
   * Return the configuration object for the leader for
   * the given service.
   */
  public ServiceInstance leader(DruidConfig service)
  {
    if (service.instances().size() == 1) {
      return service.instance();
    }
    String leader = getLeader(service.resolveService());
    return service.findHost(leader);
  }

  /**
   * Returns the leader URL for the given service.
   */
  public String getLeader(String service)
  {
    String url = StringUtils.format(
        "%s/druid/%s/v1/leader",
        config.routerUrl(),
        service
    );
    return get(url).getContent();
  }

  /**
   * Checks if a node is healthy, given the service and instance.
   *
   * @return `true` if the message returns `true`, `false` if the
   * message fails (indicating the node is not healthy.)
   */
  public boolean isHealthy(DruidConfig service, ServiceInstance instance)
  {
    return isHealthy(service.resolveUrl(config.resolveProxyHost(), instance));
  }

  /**
   * Checks if a node is healty given the URL for that node.
   *
   * @return `true` if the message returns `true`, `false` if the
   * message fails (indicating the node is not healthy.)
   */
  public boolean isHealthy(String serviceUrl)
  {
    String url = StringUtils.format(
        "%s/status/health",
        serviceUrl
    );
    return getAs(url, Boolean.class);
  }

  /**
   * Returns the URL for the lead coordinator.
   */
  public String leadCoordinatorUrl()
  {
    DruidConfig coord = config.requireCoordinator();
    ServiceInstance leader = leader(coord);
    return coord.resolveUrl(config.resolveProxyHost(), leader);
  }

  /**
   * Calls the `/v1/cluster` endpoint on the lead coordinator.
   */
  public Map<String, Object> coordinatorCluster()
  {
    String url = StringUtils.format(
        "%s/druid/coordinator/v1/cluster",
        leadCoordinatorUrl()
    );
    return getAs(url, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
  }

  /**
   * Calls the `/v1/cluster` endpoint on the router.
   */
  public Map<String, Object> routerCluster()
  {
    String url = StringUtils.format(
        "%s/druid/router/v1/cluster",
        config.routerUrl()
    );
    return getAs(url, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
  }

  /**
   * Low-level HTTP get for the given URL.
   */
  public StatusResponseHolder get(String url)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.GET, new URL(url)),
          StatusResponseHandler.getInstance()
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error from GET [%s] status [%s] content [%s]",
            url,
            response.getStatus(),
            response.getContent()
        );
      }
      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Issue a GET command and deserialize the JSON result to the given class.
   */
  public <T> T getAs(String url, Class<T> clazz)
  {
    StatusResponseHolder response = get(url);
    try {
      return jsonMapper.readValue(response.getContent(), clazz);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Issue a GET command and deserialize the JSON result to the given type reference.
   */
  public <T> T getAs(String url, TypeReference<T> typeRef)
  {
    StatusResponseHolder response = get(url);
    try {
      return jsonMapper.readValue(response.getContent(), typeRef);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Call the `/status/selfDiscovered` given a node URL.
   */
  public boolean selfDiscovered(String nodeUrl)
  {
    String url = StringUtils.format(
        "%s/status/selfDiscovered",
        nodeUrl
    );
    try {
      get(url);
    }
    catch (Exception e) {
      return false;
    }
    return true;
  }

  /**
   * Validates the cluster by waiting for each service declared in the
   * test configuration to report that it is healthy. By doing this at the
   * start of the test, individual tests don't have to retry to handle the
   * race condition that otherwise occurs between cluster and test startup.
   */
  public void validate(int timeoutMs)
  {
    for (Entry<String, DruidConfig> entry : config.requireDruid().entrySet()) {
      String name = entry.getKey();
      DruidConfig service = entry.getValue();
      for (ServiceInstance instance : service.instances()) {
        validateInstance(name, service, instance, timeoutMs);
      }
    }
  }

  /**
   * Validate an instance by waiting for it to report that it is healthy.
   */
  private void validateInstance(String name, DruidConfig service, ServiceInstance instance, int timeoutMs)
  {
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      if (isHealthy(service, instance)) {
        return;
      }
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        throw new RuntimeException("Interrupted during cluster validation");
      }
    }
    throw new RuntimeException(
        StringUtils.format("Service %s, instance %s not ready after %d ms.",
            name,
            instance.tag() == null ? "<default>" : instance.tag(),
            timeoutMs));
  }
}
