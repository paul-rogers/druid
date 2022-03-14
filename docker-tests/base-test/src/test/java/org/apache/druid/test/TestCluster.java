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

import org.apache.druid.testing.cluster.KafkaClient;
import org.apache.druid.testing.cluster.MetastoreClient;
import org.apache.druid.testing.cluster.ZooKeeperClient;
import org.apache.druid.testing.config.ClusterConfig;
import org.apache.druid.testing.config.TestConfigs;
import org.junit.Test;

/**
 * Sanity check of a Druid Docker cluster and its configuration.
 * Connects to each service to validate that the service is at
 * least listening and responding.
 */
public class TestCluster
{
  @Test
  public void testZk()
  {
    ClusterConfig config = TestConfigs.loadFromResource("/yaml/test.yaml");
    ZooKeeperClient client = new ZooKeeperClient(config);
    client.open();
    client.close();
  }

  @Test
  public void testMetastore()
  {
    ClusterConfig config = TestConfigs.loadFromResource("/yaml/test.yaml");
    MetastoreClient client = new MetastoreClient(config);
    client.open();
    client.close();
  }

  @Test
  public void testKafka()
  {
    ClusterConfig config = TestConfigs.loadFromResource("/yaml/test.yaml");
    KafkaClient client = new KafkaClient(config);
    client.open();
    client.close();
  }
}
