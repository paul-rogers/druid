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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.curator.ExhibitorConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.testing.IntegrationTestingConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

  @JsonProperty("name")
  private String name;
  @JsonProperty("dockerHost")
  private String dockerHost;
  @JsonProperty("include")
  private String include;
  @JsonProperty("zk")
  private ZKConfig zk;
  @JsonProperty("metastore")
  private MetastoreConfig metastore;
  @JsonProperty("kafka")
  private KafkaConfig kafka;
  @JsonProperty("druid")
  private Map<String, DruidConfig> druidServices;
  @JsonProperty("properties")
  private Properties properties;
  @JsonProperty("metastoreInit")
  private List<MetastoreStmt> metastoreInit;

  public static ClusterConfig loadFromFile(String filePath)
  {
    return loadFromFile(new File(filePath));
  }

  public static ClusterConfig loadFromFile(File configFile)
  {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      return mapper.readValue(configFile, ClusterConfig.class);
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
      return mapper.readValue(is, ClusterConfig.class);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to load config resource: " + resource);
    }
  }

  @JsonProperty("name")
  public String name()
  {
    return name;
  }

  @JsonProperty("dockerHost")
  public String dockerHost()
  {
    return dockerHost;
  }

  @JsonProperty("include")
  public String include()
  {
    return include;
  }

  @JsonProperty("zk")
  public ZKConfig zk()
  {
    return zk;
  }

  @JsonProperty("metastore")
  public MetastoreConfig metastore()
  {
    return metastore;
  }

  @JsonProperty("kafka")
  public KafkaConfig kafka()
  {
    return kafka;
  }

  @JsonProperty("properties")
  public Properties properties()
  {
    return properties;
  }

  @JsonProperty("metastoreInit")
  public List<MetastoreStmt> metastoreInit()
  {
    return metastoreInit;
  }

  public Properties resolveProperties()
  {
    return properties == null ? new Properties() : properties;
  }

  public String resolveDockerHost()
  {
    if (Strings.isNullOrEmpty(dockerHost)) {
      return "localhost";
    }
    return dockerHost;
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
    return CuratorConfig.create(zk.resolveDockerHosts(resolveDockerHost()));
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
    return metastore.toMetadataConfig(resolveDockerHost());
  }

  /**
   * Convert the config in this structure the the properties
   * structure
   * @return
   */
  public Properties toProperties()
  {
    Properties properties = new Properties();
    properties.put("druid.test.config.dockerIp", "localhost");
    return properties;
  }

  public IntegrationTestingConfig toIntegrationTestingConfig()
  {
    return new IntegrationTestingConfigShim();
  }

  /**
   * Adapter to the "legacy" cluster configuration used by tests.
   */
  private class IntegrationTestingConfigShim implements IntegrationTestingConfig
  {
    @Override
    public String getZookeeperHosts()
    {
      return zk.resolveDockerHosts(dockerHost);
    }

    @Override
    public String getKafkaHost()
    {
      return dockerHost;
    }

    @Override
    public String getKafkaInternalHost()
    {
      return requireKafka().resolveContainerHost();
    }

    @Override
    public String getBrokerHost()
    {
      return dockerHost;
    }

    @Override
    public String getBrokerInternalHost()
    {
      return requireBroker().resolveContainerHost();
    }

    @Override
    public String getRouterHost()
    {
      return dockerHost;
    }

    @Override
    public String getRouterInternalHost()
    {
      return requireRouter().resolveContainerHost();
    }

    @Override
    public String getCoordinatorHost()
    {
      return dockerHost;
    }

    @Override
    public String getCoordinatorInternalHost()
    {
      DruidConfig config = requireCoordinator();
      return config.resolveContainerHost(config.tagOrDefault("one"));
    }

    @Override
    public String getCoordinatorTwoInternalHost()
    {
      DruidConfig config = requireCoordinator();
      return config.resolveContainerHost(config.requireInstance("two"));
    }

    @Override
    public String getCoordinatorTwoHost()
    {
      return dockerHost;
    }

    @Override
    public String getOverlordHost()
    {
      return dockerHost;
    }

    @Override
    public String getOverlordTwoHost()
    {
      return dockerHost;
    }

    @Override
    public String getOverlordInternalHost()
    {
      DruidConfig config = requireOverlord();
      return config.resolveContainerHost(config.tagOrDefault("one"));
    }

    @Override
    public String getOverlordTwoInternalHost()
    {
      DruidConfig config = requireOverlord();
      return config.resolveContainerHost(config.requireInstance("two"));
    }

    @Override
    public String getMiddleManagerHost()
    {
      return dockerHost;
    }

    @Override
    public String getMiddleManagerInternalHost()
    {
      return requireMiddleManager().resolveContainerHost();
    }

    @Override
    public String getHistoricalHost()
    {
      return dockerHost;
    }

    @Override
    public String getHistoricalInternalHost()
    {
      return requireHistorical().resolveContainerHost();
    }

    @Override
    public String getCoordinatorUrl()
    {
      DruidConfig config = requireCoordinator();
      return config.resolveUrl(dockerHost, config.tagOrDefault("one"));
    }

    @Override
    public String getCoordinatorTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCoordinatorTwoUrl()
    {
      return requireCoordinator().resolveUrl(dockerHost, "two");
    }

    @Override
    public String getCoordinatorTwoTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getOverlordUrl()
    {
      return requireOverlord().resolveUrl(dockerHost);
    }

    @Override
    public String getOverlordTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getOverlordTwoUrl()
    {
      return requireOverlord().resolveUrl(dockerHost, "two");
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
      return indexer.resolveUrl(dockerHost);
    }

    @Override
    public String getIndexerTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getRouterUrl()
    {
      return requireRouter().resolveUrl(dockerHost);
    }

    @Override
    public String getRouterTLSUrl()
    {
      DruidConfig config = requireRouter();
      return config.resolveUrl(dockerHost, config.tagOrDefault("tls"));
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
      return requireBroker().resolveUrl(dockerHost);
    }

    @Override
    public String getBrokerTLSUrl()
    {
      DruidConfig config = requireBroker();
      return config.resolveUrl(dockerHost, config.tagOrDefault("tls"));
    }

    @Override
    public String getHistoricalUrl()
    {
      return requireHistorical().resolveUrl(dockerHost);
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
  }
}
