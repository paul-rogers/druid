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

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.guice.ConfigModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.NullHandlingModule;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.http.HttpClientModule;
import org.apache.druid.guice.security.EscalatorModule;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.IntegrationTestingCuratorConfig;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing2.cluster.MetastoreClient;

import java.util.List;
import java.util.Properties;

public class Initializer
{
  public static final String TEST_CONFIG_PROPERTY = "testConfig";
  public static final String CLUSTER_CONFIG_RESOURCE = "/yaml/%s.yaml";
  public static final String CLUSTER_CONFIG_DEFAULT = "cluster";
  public static final String METASTORE_CONFIG_PROPERTY = "sqlConfig";
  public static final String METASTORE_CONFIG_RESOURCE = "/metastore/%s.sql";
  public static final String METASTORE_CONFIG_DEFAULT = "init";

  private static class TestModule implements Module
  {
    @Override
    public void configure(Binder binder)
    {
//      binder
//          .bind(IntegrationTestingConfig.class)
//          .toProvider(IntegrationTestingConfigProvider.class)
//          .in(ManageLifecycle.class);
//      JsonConfigProvider.bind(binder, "druid.test.config", IntegrationTestingConfigProvider.class);

      binder
          .bind(CuratorConfig.class)
          .to(IntegrationTestingCuratorConfig.class);

    }

    @Provides
    @TestClient
    public HttpClient getHttpClient(
        IntegrationTestingConfig config,
        Lifecycle lifecycle,
        @Client HttpClient delegate
    )
    {
      return delegate;
    }
  }

  private final String configName;
  private final ClusterConfig clusterConfig;
  private final IntegrationTestingConfig testingConfig;
  private final Injector injector;
  private MetastoreClient metastoreClient;

  public Initializer()
  {
    this(null);
  }

  public Initializer(String configName)
  {
    this.configName = configName;
    this.clusterConfig = loadConfig(configName);
    this.testingConfig = clusterConfig.toIntegrationTestingConfig();
    this.injector = makeInjector(clusterConfig, testingConfig);
    prepareDB();
  }

  private static ClusterConfig loadConfig(String configName)
  {
    String loadPath = System.getProperty(TEST_CONFIG_PROPERTY);
    if (loadPath != null) {
      return ClusterConfig.loadFromFile(loadPath);
    }
    String baseName;
    if (configName != null) {
      baseName = configName;
    } else {
      baseName = CLUSTER_CONFIG_DEFAULT;
    }
    String loadName = StringUtils.format(CLUSTER_CONFIG_RESOURCE, baseName);
    return ClusterConfig.loadFromResource(loadName);
  }

  private static Injector makeInjector(
      ClusterConfig clusterConfig,
      IntegrationTestingConfig testingConfig)
  {
    Injector startupInjector = Guice.createInjector(
        binder -> {
          binder.bind(Properties.class).toInstance(clusterConfig.resolveProperties());
          binder.bind(DruidSecondaryModule.class);
        },
        // From GuiceInjectors
        new DruidGuiceExtensions(),
        new JacksonModule(),
        new ConfigModule(),
        new NullHandlingModule()
        );
    // Druid's code is designed to run in a Druid server. Here, we're
    // running outside a server in a client. The below is the bare
    // minimum needed to create such a client. There should be a
    // better client-oriented solution, but there isn't one at present.
    return startupInjector.createChildInjector(
        binder -> {
          binder
              .bind(ClusterConfig.class)
              .toInstance(clusterConfig);
          binder
              .bind(IntegrationTestingConfig.class)
              .toInstance(testingConfig);
        },

        // From Initialization
        new LifecycleModule(),
        new EscalatorModule(),
//        HttpClientModule.global(),
//        HttpClientModule.escalatedGlobal(),
        new HttpClientModule("druid.broker.http", Client.class),
        new HttpClientModule("druid.broker.http", EscalatedClient.class),
        new TestModule()
        );
  }

  private void prepareDB()
  {
    List<MetastoreStmt> stmts = clusterConfig.metastoreInit();
    if (stmts == null || stmts.isEmpty()) {
      return;
    }
    MetastoreClient client = metastoreClient();
    for (MetastoreStmt stmt : stmts) {
      client.execute(stmt.toSQL());
    }
  }

  public String configName()
  {
    return configName;
  }

  public Injector injector()
  {
    return injector;
  }

  public ClusterConfig clusterConfig()
  {
    return clusterConfig;
  }

  public IntegrationTestingConfig testingConfig()
  {
    return testingConfig;
  }

  public MetastoreClient metastoreClient()
  {
    if (metastoreClient == null)
    {
      metastoreClient = new MetastoreClient(clusterConfig);
    }
    return metastoreClient;
  }

}
