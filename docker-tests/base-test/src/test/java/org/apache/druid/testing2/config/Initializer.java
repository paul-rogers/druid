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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.cli.GuiceRunnable;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.curator.discovery.DiscoveryModule;
import org.apache.druid.guice.AnnouncerModule;
import org.apache.druid.guice.ConfigModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.DruidProcessingConfigModule;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.NullHandlingModule;
import org.apache.druid.guice.StorageNodeModule;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.http.HttpClientModule;
import org.apache.druid.guice.security.EscalatorModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.initialization.Initialization.ModuleList;
import org.apache.druid.jackson.DruidServiceSerializerModifier;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.jackson.StringObjectPairList;
import org.apache.druid.jackson.ToStringObjectPairListDeserializer;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.LoggingEmitter;
import org.apache.druid.java.util.emitter.core.LoggingEmitterConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing2.cluster.ClusterClient;
import org.apache.druid.testing2.cluster.MetastoreClient;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;

/**
 * The magic needed to piece together enough of Druid to allow clients to
 * run without server dependencies being pulled in. Used to set up the
 * Guice injector used to inject members into integration tests, while
 * reading configuration from the docker.yaml or similar test
 * configuration file.
 * <p>
 * Much of the work here deals the tedious task of assembling Druid
 * modules, sometimes using copy/past to grab the part that a client
 * wants (such as object deserialization) without the parts that the
 * server needs (and which would introduce the need for unused configuration
 * just to make dependencies work.)
 * <p>
 * See the documentation for these test for the "user view" of this
 * class and its configuration.
 */
public class Initializer
{
  public static final String TEST_CONFIG_PROPERTY = "testConfig";
  public static final String CLUSTER_CONFIG_RESOURCE = "/yaml/%s.yaml";
  public static final String CLUSTER_CONFIG_DEFAULT = "docker";
  public static final String METASTORE_CONFIG_PROPERTY = "sqlConfig";
  public static final String METASTORE_CONFIG_RESOURCE = "/metastore/%s.sql";
  public static final String METASTORE_CONFIG_DEFAULT = "init";

  private static final Logger log = new Logger(Initializer.class);

  private static class TestModule implements DruidModule
  {
    ClusterConfig config;

    public TestModule(ClusterConfig config)
    {
      this.config = config;
    }

    @Override
    public void configure(Binder binder)
    {
      binder
          .bind(ClusterConfig.class)
          .toInstance(config);
      binder
          .bind(IntegrationTestingConfig.class)
          .toInstance(config.toIntegrationTestingConfig());

      // Dummy DruidNode instance to make Guice happy. This instance is unused.
      binder
          .bind(DruidNode.class)
          .annotatedWith(Self.class)
          .toInstance(
              new DruidNode("integration-tests", "localhost", false, 9191, null, null, true, false));
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

    @Provides
    @ManageLifecycle
    public ServiceEmitter getServiceEmitter(ObjectMapper jsonMapper)
    {
      return new ServiceEmitter("", "", new LoggingEmitter(new LoggingEmitterConfig(), jsonMapper));
    }

    // From ServerModule to allow deserialization of DiscoveryDruidNode objects from ZK.
    // We don't want the other dependencies of that module.
    @Override
    public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
    {
      return ImmutableList.of(
          new SimpleModule()
              .addDeserializer(StringObjectPairList.class, new ToStringObjectPairListDeserializer())
              .setSerializerModifier(new DruidServiceSerializerModifier())
      );
    }
  }

  /**
   * Class used by test to identify test-specific options, load configuration
   * and "inject themselves" with dependencies.
   */
  public static class Builder
  {
    private String configName;
    private Object test;
    private List<Module> modules;
    private boolean validateCluster;
    private int timeoutMs;

    public Builder configName(String name)
    {
      this.configName = name;
      return this;
    }

    public Builder test(Object test)
    {
      this.test = test;
      return this;
    }

    public Builder modules(List<Module> modules)
    {
      this.modules = modules;
      return this;
    }

    public Builder validateCluster()
    {
      this.validateCluster = true;
      return this;
    }

    public Builder withTimeout(int timeoutMs)
    {
      this.timeoutMs = timeoutMs;
      return this;
    }

    public Initializer build()
    {
      Initializer initializer = new Initializer(this);
      if (validateCluster) {
        ClusterClient client = initializer.clusterClient();
        if (timeoutMs == 0) {
          timeoutMs = initializer.clusterConfig().resolveReadyTimeoutSec() * 1000;
        }
        client.validate(timeoutMs);
      }
      return initializer;
    }
  }

  private final String configName;
  private final ClusterConfig clusterConfig;
  private final Injector injector;
  private final Lifecycle lifecycle;
  private MetastoreClient metastoreClient;
  private ClusterClient clusterClient;

  private Initializer(Builder builder)
  {
    this.configName = builder.configName;
    this.clusterConfig = loadConfig(configName);
    this.injector = makeInjector(clusterConfig, builder.modules);
    if (builder.test != null) {
      this.injector.injectMembers(builder.test);
    }
    this.lifecycle = GuiceRunnable.initLifecycle(injector, log);
    this.clusterClient = this.injector.getInstance(ClusterClient.class);
    prepareDB();
  }

  public static Builder builder()
  {
    return new Builder();
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
    ClusterConfig config = ClusterConfig.loadFromResource(loadName);
    return config.resolveIncludes();
  }

  private static Injector makeInjector(
      ClusterConfig clusterConfig,
      List<Module> modules
  )
  {

    Injector startupInjector = Guice.createInjector(
        binder -> {
          // Use the test-provided properties rather than the usual files
          binder.bind(Properties.class).toInstance(clusterConfig.toProperties());
          binder.bind(DruidSecondaryModule.class);
          binder.bind(ClusterConfig.class).toInstance(clusterConfig);
        },
        // From GuiceInjectors
        new DruidGuiceExtensions(),
        // For serialization
        new JacksonModule(),
        new ConfigModule(),
        // To run SQL queries
        new NullHandlingModule()
        );
    ModuleList druidModules = new ModuleList(startupInjector, new HashSet<>());
    druidModules.addModules(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
          }
        },
        DruidSecondaryModule.class,
        // From Initialization
        // Required by clients
        new LifecycleModule(),
        // Required by clients
        new EscalatorModule(),
        HttpClientModule.global(),
        HttpClientModule.escalatedGlobal(),
        new HttpClientModule("druid.broker.http", Client.class),
        new HttpClientModule("druid.broker.http", EscalatedClient.class),
        // For ZK discovery
        new CuratorModule(),
        new AnnouncerModule(),
        new DiscoveryModule(),
        // Dependencies from other modules
        new DruidProcessingConfigModule(),
        // Dependencies from other modules
        new StorageNodeModule(),

        // Test-specific items, including bits copy/pasted
        // from modules that don't play well in a client setting.
        new TestModule(clusterConfig)
    );
    if (modules != null) {
      druidModules.addModules(modules);
    }
    return Guice.createInjector(druidModules.getModules());
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

  public MetastoreClient metastoreClient()
  {
    if (metastoreClient == null) {
      metastoreClient = new MetastoreClient(clusterConfig);
    }
    return metastoreClient;
  }

  public ClusterClient clusterClient()
  {
    return clusterClient;
  }

  public void close()
  {
    lifecycle.stop();
  }
}
