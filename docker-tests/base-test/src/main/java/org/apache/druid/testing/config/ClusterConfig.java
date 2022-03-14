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
import com.google.common.base.Strings;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.curator.ExhibitorConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;

import java.util.Collections;

/**
 * Java representation of the test configuration YAML.
 * <p>
 * This object is primarily de-serialized as the files are written by hand
 * to define a test. It is serialized only for debugging.
 */
public class ClusterConfig
{
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

  public String resolveDockerHost()
  {
    if (Strings.isNullOrEmpty(dockerHost)) {
      return "localhost";
    }
    return dockerHost;
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
}
