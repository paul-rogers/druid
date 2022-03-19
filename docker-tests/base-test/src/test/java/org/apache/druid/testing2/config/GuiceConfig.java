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
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.NullHandlingModule;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.http.HttpClientModule;
import org.apache.druid.guice.security.EscalatorModule;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.IntegrationTestingConfigProvider;
import org.apache.druid.testing.IntegrationTestingCuratorConfig;
import org.apache.druid.testing.guice.TestClient;

import java.util.Properties;

public class GuiceConfig
{
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

  private ClusterConfig clusterConfig;
  private IntegrationTestingConfig testingConfig;
  private Injector injector;

  public GuiceConfig()
  {
    this(null);
  }

  public GuiceConfig(String configName)
  {
    this.clusterConfig = loadConfig(configName);
    this.testingConfig = clusterConfig.toIntegrationTestingConfig();
    this.injector = makeInjector(clusterConfig, testingConfig);
  }

  private static ClusterConfig loadConfig(String configName)
  {
    if (configName == null) {
      configName = System.getProperty("testConfig");
    }
    if (configName != null) {
      return TestConfigs.loadFromFile(configName);
    }
    return TestConfigs.loadFromResource("/yaml/cluster.yaml");
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

}
