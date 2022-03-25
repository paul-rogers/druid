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

import com.google.inject.Injector;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.clients.SqlResourceTestClient;
import org.apache.druid.testing2.cluster.KafkaClient;
import org.apache.druid.testing2.cluster.MetastoreClient;
import org.apache.druid.testing2.cluster.ZooKeeperClient;
import org.apache.druid.testing2.config.ClusterConfig;
import org.apache.druid.testing2.config.Initializer;
import org.apache.druid.testing2.utils.DruidClusterAdminClient;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Ad-hoc tests to exercise the various test-specific
 * clients.
 */
public class TestClients
{

  @Test
  @Ignore("manual check")
  public void testAdminClient()
  {
    Initializer guiceConfig = Initializer.builder().build();
    Injector injector = guiceConfig.injector();
    DruidClusterAdminClient druidClusterAdminClient = injector.getInstance(DruidClusterAdminClient.class);
    druidClusterAdminClient.waitUntilBrokerReady();
  }

  @Test
  @Ignore("manual check")
  public void testQueryClient()
  {
    Initializer guiceConfig = Initializer.builder().build();
    Injector injector = guiceConfig.injector();
    SqlResourceTestClient client = injector.getInstance(SqlResourceTestClient.class);
    String url = "http://localhost:8082/druid/v2/sql";
    String stmt = "SELECT COUNT(*) AS cnt FROM sys.servers";
    SqlQuery query = new SqlQuery(stmt, null, false, false, false, null, null);
    List<Map<String, Object>> results = client.query(url, query);
    assertEquals(1, results.size());
    assertTrue((Integer) results.get(0).get("cnt") > 1);
  }

  @Test
  @Ignore("manual check")
  public void testMetastore()
  {
    ClusterConfig config = ClusterConfig.loadFromResource("/yaml/test.yaml");
    MetastoreClient client = new MetastoreClient(config);
    client.close();
  }

  @Test
  @Ignore("manual check")
  public void testKafka()
  {
    ClusterConfig config = ClusterConfig.loadFromResource("/yaml/test.yaml");
    KafkaClient client = new KafkaClient(config);
    client.open();
    client.close();
  }

  @Test
  @Ignore("manual check")
  public void testZkClient() throws Exception
  {
    Initializer guiceConfig = Initializer.builder().configName("test").build();
    ZooKeeperClient client = new ZooKeeperClient(guiceConfig.clusterConfig());
    CuratorFramework curator = client.curator();
    List<String> nodes = curator.getChildren().forPath("/druid/internal-discovery");
    for (String node : nodes) {
      List<String> members = curator.getChildren().forPath("/druid/internal-discovery/" + node);
      System.out.println("Role: " + node + ", members: " + members.toString());
    }
    client.close();
  }
}
