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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.curator.ExhibitorConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.testing.IntegrationTestingConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Java representation of the test configuration YAML.
 * <p>
 * This object is primarily de-serialized as the files are written by hand
 * to define a test. It is serialized only for debugging.
 */
public class ClusterConfig
{
  public static final String COORDINATOR = "coordinator";
  public static final String HISTORICAL = "historical";
  public static final String OVERLORD = "overlord";
  public static final String BROKER = "broker";
  public static final String ROUTER = "router";
  public static final String MIDDLEMANAGER = "middlemanager";
  public static final String INDEXER = "indexer";

  public static final int DEFAULT_READY_TIMEOUT_SEC = 60;

  private boolean isResource;
  private String basePath;

  @JsonProperty("proxyHost")
  private String proxyHost;
  @JsonProperty("include")
  private List<String> include;
  @JsonProperty("readyTimeoutSec")
  private int readyTimeoutSec;
  @JsonProperty("zk")
  private ZKConfig zk;
  @JsonProperty("metastore")
  private MetastoreConfig metastore;
  @JsonProperty("kafka")
  private KafkaConfig kafka;
  @JsonProperty("druid")
  private Map<String, DruidConfig> druidServices;
  @JsonProperty("properties")
  private Map<String, Object> properties;
  @JsonProperty("metastoreInit")
  private List<MetastoreStmt> metastoreInit;

  public ClusterConfig()
  {
  }

  public ClusterConfig(ClusterConfig from)
  {
    this.isResource = from.isResource;
    this.basePath = from.basePath;
    this.proxyHost = from.proxyHost;
    if (from.include != null) {
      this.include = new ArrayList<>(from.include);
    }
    this.zk = from.zk;
    this.metastore = from.metastore;
    this.kafka = from.kafka;
    if (from.druidServices != null) {
      this.druidServices = new HashMap<>(from.druidServices);
    }
    if (from.properties != null) {
      this.properties = new HashMap<>(from.properties);
    }
    if (from.metastoreInit != null) {
      this.metastoreInit = new ArrayList<>(from.metastoreInit);
    }
  }

  public static ClusterConfig loadFromFile(String filePath)
  {
    return loadFromFile(new File(filePath));
  }

  public static ClusterConfig loadFromFile(File configFile)
  {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      ClusterConfig config = mapper.readValue(configFile, ClusterConfig.class);
      config.isResource = false;
      config.basePath = configFile.getParent();
      return config;
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to load config file: " + configFile.toString());
    }
  }

  public static ClusterConfig loadFromResource(String resource)
  {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try (InputStream is = TestConfigs.class.getResourceAsStream(resource)) {
      if (is == null) {
        throw new ISE("Config resource not found: " + resource);
      }
      ClusterConfig config = mapper.readValue(is, ClusterConfig.class);
      config.isResource = true;
      return config;
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to load config resource: " + resource);
    }
  }

  public ClusterConfig resolveIncludes()
  {
    if (include == null || include.isEmpty()) {
      return this;
    }
    ClusterConfig included = null;
    for (String entry : include) {
      ClusterConfig child = loadInclude(entry);
      if (included == null) {
        included = child;
      } else {
        included = included.merge(child);
      }
    }
    return included.merge(this);
  }

  private ClusterConfig loadInclude(String includeName)
  {
    if (isResource) {
      return loadFromResource(includeName);
    } else {
      File file = new File(new File(basePath), includeName);
      return loadFromFile(file);
    }
  }

  @JsonProperty("readyTimeoutSec")
  @JsonInclude(Include.NON_DEFAULT)
  public int readyTimeoutSec()
  {
    return readyTimeoutSec;
  }

  @JsonProperty("proxyHost")
  @JsonInclude(Include.NON_NULL)
  public String proxyHost()
  {
    return proxyHost;
  }

  @JsonProperty("include")
  @JsonInclude(Include.NON_NULL)
  public List<String> include()
  {
    return include;
  }

  @JsonProperty("zk")
  @JsonInclude(Include.NON_NULL)
  public ZKConfig zk()
  {
    return zk;
  }

  @JsonProperty("metastore")
  @JsonInclude(Include.NON_NULL)
  public MetastoreConfig metastore()
  {
    return metastore;
  }

  @JsonProperty("kafka")
  @JsonInclude(Include.NON_NULL)
  public KafkaConfig kafka()
  {
    return kafka;
  }

  @JsonProperty("properties")
  @JsonInclude(Include.NON_NULL)
  public Map<String, Object> properties()
  {
    return properties;
  }

  @JsonProperty("metastoreInit")
  @JsonInclude(Include.NON_NULL)
  public List<MetastoreStmt> metastoreInit()
  {
    return metastoreInit;
  }

  public int resolveReadyTimeoutSec()
  {
    return readyTimeoutSec > 0 ? readyTimeoutSec : DEFAULT_READY_TIMEOUT_SEC;
  }

  public String resolveProxyHost()
  {
    if (Strings.isNullOrEmpty(proxyHost)) {
      return "localhost";
    }
    return proxyHost;
  }

  public Map<String, DruidConfig> requireDruid()
  {
    if (druidServices == null) {
      throw new ISE("Please configure Druid services");
    }
    return druidServices;
  }

  public MetastoreConfig requireMetastore()
  {
    if (metastore == null) {
      throw new ISE("Please specify the Metastore configuration");
    }
    return metastore;
  }

  public KafkaConfig requireKafka()
  {
    if (kafka == null) {
      throw new ISE("Please specify the Kafka configuration");
    }
    return kafka;
  }

  public DruidConfig druidService(String serviceKey)
  {
    DruidConfig service = requireDruid().get(serviceKey);
    if (service != null) {
      service.setServiceKey(serviceKey);
    }
    return service;
  }

  public DruidConfig requireService(String serviceKey)
  {
    DruidConfig service = druidService(serviceKey);
    if (service == null) {
      throw new ISE("Please configure Druid service " + serviceKey);
    }
    return service;
  }

  public DruidConfig requireCoordinator()
  {
    return requireService(COORDINATOR);
  }

  public DruidConfig requireOverlord()
  {
    return requireService(OVERLORD);
  }

  public DruidConfig requireBroker()
  {
    return requireService(BROKER);
  }

  public DruidConfig requireRouter()
  {
    return requireService(ROUTER);
  }

  public DruidConfig requireMiddleManager()
  {
    return requireService(MIDDLEMANAGER);
  }

  public DruidConfig requireHistorical()
  {
    return requireService(HISTORICAL);
  }

  public String routerUrl()
  {
    return requireRouter().resolveUrl(resolveProxyHost());
  }

  @Override
  public String toString()
  {
    return TestConfigs.toYaml(this);
  }

  public CuratorConfig toCuratorConfig()
  {
    if (zk == null) {
      throw new ISE("ZooKeeper not configured");
    }
    // TODO: Add a builder for other properties
    return CuratorConfig.create(zk.resolveDockerHosts(resolveProxyHost()));
  }

  public ExhibitorConfig toExhibitorConfig()
  {
    // Does not yet support exhibitors
    return ExhibitorConfig.create(Collections.emptyList());
  }

  public MetadataStorageConnectorConfig toMetadataConfig()
  {
    if (metastore == null) {
      throw new ISE("Metastore not configured");
    }
    return metastore.toMetadataConfig(resolveProxyHost());
  }

  /**
   * Convert the config in this structure the the properties
   * structure
   */
  public Properties toProperties()
  {
    Properties properties = new Properties();
    properties.put("druid.test.config.dockerIp", resolveProxyHost());
    /*
     * We will use this instead of druid server's CuratorConfig, because CuratorConfig in
     * a test cluster environment sees zookeeper at localhost even if zookeeper is elsewhere.
     * We'll take the zookeeper host from the configuration file instead.
     */
    properties.put(
        CuratorModule.CURATOR_CONFIG_PREFIX + ".zkHosts",
        zk.resolveDockerHosts(proxyHost));
    if (this.properties != null) {
      for (Entry<String, Object> entry : this.properties.entrySet()) {
        properties.put(entry.getKey(), entry.getValue().toString());
      }
    }
    return properties;
  }

  public IntegrationTestingConfig toIntegrationTestingConfig()
  {
    return new IntegrationTestingConfigShim();
  }

  public ClusterConfig merge(ClusterConfig overrides)
  {
    ClusterConfig merged = new ClusterConfig(this);
    if (overrides.readyTimeoutSec != 0) {
      merged.readyTimeoutSec = overrides.readyTimeoutSec;
    }
    if (overrides.proxyHost != null) {
      merged.proxyHost = overrides.proxyHost;
    }
    // Includes are already considered.
    if (overrides.zk != null) {
      merged.zk = overrides.zk;
    }
    if (overrides.metastore != null) {
      merged.metastore = overrides.metastore;
    }
    if (overrides.kafka != null) {
      merged.kafka = overrides.kafka;
    }
    if (merged.druidServices == null) {
      merged.druidServices = overrides.druidServices;
    } else if (overrides.druidServices != null) {
      merged.druidServices.putAll(overrides.druidServices);
    }
    if (merged.properties == null) {
      merged.properties = overrides.properties;
    } else if (overrides.properties != null) {
      merged.properties.putAll(overrides.properties);
    }
    if (merged.metastoreInit == null) {
      merged.metastoreInit = overrides.metastoreInit;
    } else if (overrides.metastoreInit != null) {
      merged.metastoreInit.addAll(overrides.metastoreInit);
    }
    return merged;
  }

  /**
   * Adapter to the "legacy" cluster configuration used by tests.
   */
  private class IntegrationTestingConfigShim implements IntegrationTestingConfig
  {
    @Override
    public String getZookeeperHosts()
    {
      return zk.resolveDockerHosts(proxyHost);
    }

    @Override
    public String getKafkaHost()
    {
      return resolveProxyHost();
    }

    @Override
    public String getKafkaInternalHost()
    {
      return requireKafka().resolveContainerHost();
    }

    @Override
    public String getBrokerHost()
    {
      return resolveProxyHost();
    }

    @Override
    public String getBrokerInternalHost()
    {
      return requireBroker().resolveHost();
    }

    @Override
    public String getRouterHost()
    {
      return resolveProxyHost();
    }

    @Override
    public String getRouterInternalHost()
    {
      return requireRouter().resolveHost();
    }

    @Override
    public String getCoordinatorHost()
    {
      return resolveProxyHost();
    }

    @Override
    public String getCoordinatorInternalHost()
    {
      DruidConfig config = requireCoordinator();
      return config.resolveHost(config.tagOrDefault("one"));
    }

    @Override
    public String getCoordinatorTwoInternalHost()
    {
      DruidConfig config = requireCoordinator();
      return config.resolveHost(config.requireInstance("two"));
    }

    @Override
    public String getCoordinatorTwoHost()
    {
      return resolveProxyHost();
    }

    @Override
    public String getOverlordHost()
    {
      return resolveProxyHost();
    }

    @Override
    public String getOverlordTwoHost()
    {
      return resolveProxyHost();
    }

    @Override
    public String getOverlordInternalHost()
    {
      DruidConfig config = requireOverlord();
      return config.resolveHost(config.tagOrDefault("one"));
    }

    @Override
    public String getOverlordTwoInternalHost()
    {
      DruidConfig config = requireOverlord();
      return config.resolveHost(config.requireInstance("two"));
    }

    @Override
    public String getMiddleManagerHost()
    {
      return resolveProxyHost();
    }

    @Override
    public String getMiddleManagerInternalHost()
    {
      return requireMiddleManager().resolveHost();
    }

    @Override
    public String getHistoricalHost()
    {
      return resolveProxyHost();
    }

    @Override
    public String getHistoricalInternalHost()
    {
      return requireHistorical().resolveHost();
    }

    @Override
    public String getCoordinatorUrl()
    {
      DruidConfig config = requireCoordinator();
      return config.resolveUrl(resolveProxyHost(), config.tagOrDefault("one"));
    }

    @Override
    public String getCoordinatorTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCoordinatorTwoUrl()
    {
      return requireCoordinator().resolveUrl(resolveProxyHost(), "two");
    }

    @Override
    public String getCoordinatorTwoTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getOverlordUrl()
    {
      return requireOverlord().resolveUrl(resolveProxyHost());
    }

    @Override
    public String getOverlordTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getOverlordTwoUrl()
    {
      return requireOverlord().resolveUrl(resolveProxyHost(), "two");
    }

    @Override
    public String getOverlordTwoTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getIndexerUrl()
    {
      DruidConfig indexer = druidService(INDEXER);
      if (indexer == null) {
        indexer = requireMiddleManager();
      }
      return indexer.resolveUrl(resolveProxyHost());
    }

    @Override
    public String getIndexerTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getRouterUrl()
    {
      return routerUrl();
    }

    @Override
    public String getRouterTLSUrl()
    {
      DruidConfig config = requireRouter();
      return config.resolveUrl(resolveProxyHost(), config.tagOrDefault("tls"));
    }

    @Override
    public String getPermissiveRouterUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getPermissiveRouterTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getNoClientAuthRouterUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getNoClientAuthRouterTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCustomCertCheckRouterUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCustomCertCheckRouterTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getBrokerUrl()
    {
      return requireBroker().resolveUrl(resolveProxyHost());
    }

    @Override
    public String getBrokerTLSUrl()
    {
      DruidConfig config = requireBroker();
      return config.resolveUrl(resolveProxyHost(), config.tagOrDefault("tls"));
    }

    @Override
    public String getHistoricalUrl()
    {
      return requireHistorical().resolveUrl(resolveProxyHost());
    }

    @Override
    public String getHistoricalTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getProperty(String prop)
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getUsername()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getPassword()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public Map<String, String> getProperties()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public boolean manageKafkaTopic()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getExtraDatasourceNameSuffix()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCloudBucket()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCloudPath()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCloudRegion()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getS3AssumeRoleWithExternalId()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getS3AssumeRoleExternalId()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getS3AssumeRoleWithoutExternalId()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getAzureKey()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getHadoopGcsCredentialsPath()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getStreamEndpoint()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getSchemaRegistryHost()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public boolean isDocker()
    {
      return true;
    }

    @Override
    public String getDockerHost()
    {
      return resolveProxyHost();
    }
  }
}
