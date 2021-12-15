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

package org.apache.druid.server.initialization.jetty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.fasterxml.jackson.jaxrs.smile.JacksonSmileProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.WebConfig;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.JSR311Resource;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.java.util.metrics.MonitorUtils;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.StatusResource;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.TLSServerConfig;
import org.apache.druid.server.metrics.DataSourceTaskIdHolder;
import org.apache.druid.server.metrics.MetricsModule;
import org.apache.druid.server.metrics.MonitorsConfig;
import org.apache.druid.server.security.CustomCheckX509TrustManager;
import org.apache.druid.server.security.TLSCertificateChecker;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.jboss.netty.handler.codec.http.HttpVersion;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.KeyStore;
import java.security.cert.CRL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class JettyServerModule extends JerseyServletModule
{
  private static final Logger log = new Logger(JettyServerModule.class);

  private static final AtomicInteger ACTIVE_CONNECTIONS = new AtomicInteger();
  private static final String HTTP_1_1_STRING = HttpVersion.HTTP_1_1.getText();
  private static QueuedThreadPool jettyServerThreadPool = null;

  @Override
  protected void configureServlets()
  {
    Binder binder = binder();

    JsonConfigProvider.bind(binder, "druid.server.http", ServerConfig.class);
    JsonConfigProvider.bind(binder, "druid.server.https", TLSServerConfig.class);

    binder.bind(GuiceContainer.class).to(DruidGuiceContainer.class);
    binder.bind(DruidGuiceContainer.class).in(Scopes.SINGLETON);
    binder.bind(CustomExceptionMapper.class).in(Singleton.class);
    binder.bind(ForbiddenExceptionMapper.class).in(Singleton.class);
    binder.bind(BadRequestExceptionMapper.class).in(Singleton.class);
    binder.bind(ServiceUnavailableExceptionMapper.class).in(Singleton.class);

    serve("/*").with(DruidGuiceContainer.class);

    Jerseys.addResource(binder, StatusResource.class);
    binder.bind(StatusResource.class).in(LazySingleton.class);

    // Adding empty binding for ServletFilterHolders and Handlers so that injector returns an empty set if none
    // are provided by extensions.
    Multibinder.newSetBinder(binder, Handler.class);
    Multibinder.newSetBinder(binder, ServletFilterHolder.class);

    MetricsModule.register(binder, JettyMonitor.class);
  }

  @SuppressWarnings("serial")
  public static class DruidGuiceContainer extends GuiceContainer
  {
    private final Set<Class<?>> resources;

    @Inject
    public DruidGuiceContainer(
        Injector injector,
        @JSR311Resource Set<Class<?>> resources
    )
    {
      super(injector);
      this.resources = resources;
    }

    @Override
    protected ResourceConfig getDefaultResourceConfig(
        Map<String, Object> props, WebConfig webConfig
    )
    {
      return new DefaultResourceConfig(resources);
    }
  }

  @Provides
  @LazySingleton
  public Server getServer(
      final Injector injector,
      final Lifecycle lifecycle,
      @Self final DruidNode node,
      final ServerConfig config,
      final TLSServerConfig TLSServerConfig
  )
  {
    return makeAndInitializeServer(
        injector,
        lifecycle,
        node,
        config,
        TLSServerConfig,
        injector.getExistingBinding(Key.get(SslContextFactory.Server.class)),
        injector.getInstance(TLSCertificateChecker.class)
    );
  }

  @Provides
  @Singleton
  public JacksonJsonProvider getJacksonJsonProvider(@Json ObjectMapper objectMapper)
  {
    final JacksonJsonProvider provider = new JacksonJsonProvider();
    provider.setMapper(objectMapper);
    return provider;
  }

  @Provides
  @Singleton
  public JacksonSmileProvider getJacksonSmileProvider(@Smile ObjectMapper objectMapper)
  {
    final JacksonSmileProvider provider = new JacksonSmileProvider();
    provider.setMapper(objectMapper);
    return provider;
  }

  static Server makeAndInitializeServer(
      Injector injector,
      Lifecycle lifecycle,
      DruidNode node,
      ServerConfig config,
      TLSServerConfig tlsServerConfig,
      Binding<SslContextFactory.Server> sslContextFactoryBinding,
      TLSCertificateChecker certificateChecker
  )
  {
    JettyServerBuilder builder = new JettyServerBuilder(
        injector,
        lifecycle,
        node,
        config,
        tlsServerConfig,
        sslContextFactoryBinding,
        certificateChecker
        );
    Server server = builder.build();
    setJettyServerThreadPool(builder.threadPool());
    return server;
  }

  public static class JettyServerBuilder
  {
    private final Injector injector;
    private final Lifecycle lifecycle;
    private final DruidNode node;
    private final ServerConfig config;
    private final TLSServerConfig tlsServerConfig;
    private final Binding<SslContextFactory.Server> sslContextFactoryBinding;
    private final TLSCertificateChecker certificateChecker;
    private QueuedThreadPool threadPool;
    private Server server;
    private SslContextFactory.Server sslContextFactory;

    public JettyServerBuilder(
        Injector injector,
        Lifecycle lifecycle,
        DruidNode node,
        ServerConfig config,
        TLSServerConfig tlsServerConfig,
        Binding<SslContextFactory.Server> sslContextFactoryBinding,
        TLSCertificateChecker certificateChecker
    )
    {
      this.injector = injector;
      this.lifecycle = lifecycle;
      this.node = node;
      this.config = config;
      this.tlsServerConfig = tlsServerConfig;
      this.sslContextFactoryBinding = sslContextFactoryBinding;
      this.certificateChecker = certificateChecker;
    }

    private ThreadPool buildThreadPool()
    {
      // adjusting to make config.getNumThreads() mean, "number of threads
      // that concurrently handle the requests".
      int numServerThreads = config.getNumThreads() + getMaxJettyAcceptorsSelectorsNum();

      // Minimum thread pool size. Defaults to the number of threads for backward
      // compatibility. Else, it is the provided number up to the total number.
      // A smaller minimum means faster startup which benefits debugging and testing,
      // at the cost of slower initial responses when threads are created on demand.
      int minThreads = config.getMinThreads();
      if (minThreads < 0) {
        minThreads = numServerThreads;
      } else {
        minThreads = Math.min(numServerThreads, minThreads);
      }

      if (config.getQueueSize() == Integer.MAX_VALUE) {
        threadPool = new QueuedThreadPool();
        threadPool.setMinThreads(minThreads);
        threadPool.setMaxThreads(numServerThreads);
      } else {
        threadPool = new QueuedThreadPool(
            minThreads,
            numServerThreads,
            60000, // same default is used in other case when threadPool = new QueuedThreadPool()
            new LinkedBlockingQueue<>(config.getQueueSize())
        );
      }

      threadPool.setDaemon(true);
      return threadPool;
    }

    private int getMaxJettyAcceptorsSelectorsNum()
    {
      // This computation is based on Jetty v9.3.19 which uses up to 8 (4 acceptors and 4 selectors) threads per
      // ServerConnector
      int numServerConnector = (node.isEnablePlaintextPort() ? 1 : 0) + (node.isEnableTlsPort() ? 1 : 0);
      return numServerConnector * 8;
    }

    public QueuedThreadPool threadPool()
    {
      return threadPool;
    }

    public ServerConnector buildPlainTextConnector()
    {
      log.info("Creating http connector with port [%d]", node.getPlaintextPort());
      HttpConfiguration httpConfiguration = new HttpConfiguration();
      if (config.isEnableForwardedRequestCustomizer()) {
        httpConfiguration.addCustomizer(new ForwardedRequestCustomizer());
      }

      httpConfiguration.setRequestHeaderSize(config.getMaxRequestHeaderSize());
      httpConfiguration.setSendServerVersion(false);
      final ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(httpConfiguration));
      if (node.isBindOnHost()) {
        connector.setHost(node.getHost());
      }
      connector.setPort(node.getPlaintextPort());
      return connector;
    }

    public void buildSslContextFactory()
    {
      if (sslContextFactoryBinding != null) {
        sslContextFactory = sslContextFactoryBinding.getProvider().get();
        return;
      }
      // Never trust all certificates by default
      sslContextFactory = new IdentityCheckOverrideSslContextFactory(tlsServerConfig, certificateChecker);

      sslContextFactory.setKeyStorePath(tlsServerConfig.getKeyStorePath());
      sslContextFactory.setKeyStoreType(tlsServerConfig.getKeyStoreType());
      sslContextFactory.setKeyStorePassword(tlsServerConfig.getKeyStorePasswordProvider().getPassword());
      sslContextFactory.setCertAlias(tlsServerConfig.getCertAlias());
      sslContextFactory.setKeyManagerFactoryAlgorithm(tlsServerConfig.getKeyManagerFactoryAlgorithm() == null
                                                      ? KeyManagerFactory.getDefaultAlgorithm()
                                                      : tlsServerConfig.getKeyManagerFactoryAlgorithm());
      sslContextFactory.setKeyManagerPassword(tlsServerConfig.getKeyManagerPasswordProvider() == null ?
                                              null : tlsServerConfig.getKeyManagerPasswordProvider().getPassword());
      if (tlsServerConfig.getIncludeCipherSuites() != null) {
        sslContextFactory.setIncludeCipherSuites(
            tlsServerConfig.getIncludeCipherSuites().toArray(new String[0]));
      }
      if (tlsServerConfig.getExcludeCipherSuites() != null) {
        sslContextFactory.setExcludeCipherSuites(
            tlsServerConfig.getExcludeCipherSuites().toArray(new String[0]));
      }
      if (tlsServerConfig.getIncludeProtocols() != null) {
        sslContextFactory.setIncludeProtocols(
            tlsServerConfig.getIncludeProtocols().toArray(new String[0]));
      }
      if (tlsServerConfig.getExcludeProtocols() != null) {
        sslContextFactory.setExcludeProtocols(
            tlsServerConfig.getExcludeProtocols().toArray(new String[0]));
      }

      sslContextFactory.setNeedClientAuth(tlsServerConfig.isRequireClientCertificate());
      sslContextFactory.setWantClientAuth(tlsServerConfig.isRequestClientCertificate());
      if (tlsServerConfig.isRequireClientCertificate() || tlsServerConfig.isRequestClientCertificate()) {
        if (tlsServerConfig.getCrlPath() != null) {
          // setValidatePeerCerts is used just to enable revocation checking using a static CRL file.
          // Certificate validation is always performed when client certificates are required.
          sslContextFactory.setValidatePeerCerts(true);
          sslContextFactory.setCrlPath(tlsServerConfig.getCrlPath());
        }
        if (tlsServerConfig.isValidateHostnames()) {
          sslContextFactory.setEndpointIdentificationAlgorithm("HTTPS");
        }
        if (tlsServerConfig.getTrustStorePath() != null) {
          sslContextFactory.setTrustStorePath(tlsServerConfig.getTrustStorePath());
          sslContextFactory.setTrustStoreType(
              tlsServerConfig.getTrustStoreType() == null
              ? KeyStore.getDefaultType()
              : tlsServerConfig.getTrustStoreType()
          );
          sslContextFactory.setTrustManagerFactoryAlgorithm(
              tlsServerConfig.getTrustStoreAlgorithm() == null
              ? TrustManagerFactory.getDefaultAlgorithm()
              : tlsServerConfig.getTrustStoreAlgorithm()
          );
          sslContextFactory.setTrustStorePassword(
              tlsServerConfig.getTrustStorePasswordProvider() == null
              ? null
              : tlsServerConfig.getTrustStorePasswordProvider().getPassword()
          );
        }
      }
    }

    public ServerConnector buildTlsConnector()
    {
      log.info("Creating https connector with port [%d]", node.getTlsPort());
      buildSslContextFactory();

      final HttpConfiguration httpsConfiguration = new HttpConfiguration();
      if (config.isEnableForwardedRequestCustomizer()) {
        httpsConfiguration.addCustomizer(new ForwardedRequestCustomizer());
      }
      httpsConfiguration.setSecureScheme("https");
      httpsConfiguration.setSecurePort(node.getTlsPort());
      httpsConfiguration.addCustomizer(new SecureRequestCustomizer());
      httpsConfiguration.setRequestHeaderSize(config.getMaxRequestHeaderSize());
      httpsConfiguration.setSendServerVersion(false);
      final ServerConnector connector = new ServerConnector(
          server,
          new SslConnectionFactory(sslContextFactory, HTTP_1_1_STRING),
          new HttpConnectionFactory(httpsConfiguration)
      );
      if (node.isBindOnHost()) {
        connector.setHost(node.getHost());
      }
      connector.setPort(node.getTlsPort());
      return connector;
    }

    private void buildConnectors()
    {
      final List<ServerConnector> serverConnectors = new ArrayList<>();

      if (node.isEnablePlaintextPort()) {
        serverConnectors.add(buildPlainTextConnector());
      }
      if (node.isEnableTlsPort()) {
        serverConnectors.add(buildTlsConnector());
      } else {
        sslContextFactory = null;
      }

      final ServerConnector[] connectors = new ServerConnector[serverConnectors.size()];
      int index = 0;
      for (ServerConnector connector : serverConnectors) {
        connectors[index++] = connector;
        connector.setIdleTimeout(Ints.checkedCast(config.getMaxIdleTime().toStandardDuration().getMillis()));
        // workaround suggested in -
        // https://bugs.eclipse.org/bugs/show_bug.cgi?id=435322#c66 for Jetty half open connection issues during failovers
        connector.setAcceptorPriorityDelta(-1);

        List<ConnectionFactory> monitoredConnFactories = new ArrayList<>();
        for (ConnectionFactory cf : connector.getConnectionFactories()) {
          // we only want to monitor the first connection factory, since it will pass the connection to subsequent
          // connection factories (in this case HTTP/1.1 after the connection is unencrypted for SSL)
          if (cf.getProtocol().equals(connector.getDefaultProtocol())) {
            monitoredConnFactories.add(new JettyMonitoringConnectionFactory(cf, ACTIVE_CONNECTIONS));
          } else {
            monitoredConnFactories.add(cf);
          }
        }
        connector.setConnectionFactories(monitoredConnFactories);
      }

      server.setConnectors(connectors);
    }

    private void buildLifecycleListener()
    {
      server.addLifeCycleListener(new LifeCycle.Listener()
      {
        @Override
        public void lifeCycleStarting(LifeCycle event)
        {
          log.debug("Jetty lifecycle starting [%s]", event.getClass());
        }

        @Override
        public void lifeCycleStarted(LifeCycle event)
        {
          log.debug("Jetty lifeycle started [%s]", event.getClass());
        }

        @Override
        public void lifeCycleFailure(LifeCycle event, Throwable cause)
        {
          log.error(cause, "Jetty lifecycle event failed [%s]", event.getClass());
        }

        @Override
        public void lifeCycleStopping(LifeCycle event)
        {
          log.debug("Jetty lifecycle stopping [%s]", event.getClass());
        }

        @Override
        public void lifeCycleStopped(LifeCycle event)
        {
          log.debug("Jetty lifecycle stopped [%s]", event.getClass());
        }
      });
    }

    private void buildLifecyleHandler()
    {
      lifecycle.addHandler(new Lifecycle.Handler()
      {
        @Override
        public void start() throws Exception
        {
          log.debug("Starting Jetty Server...");
          server.start();
          if (node.isEnableTlsPort()) {
            // Perform validation
            Preconditions.checkNotNull(sslContextFactory);
            final SSLEngine sslEngine = sslContextFactory.newSSLEngine();
            if (sslEngine.getEnabledCipherSuites() == null || sslEngine.getEnabledCipherSuites().length == 0) {
              throw new ISE(
                  "No supported cipher suites found, supported suites [%s], configured suites include list: [%s] exclude list: [%s]",
                  Arrays.toString(sslEngine.getSupportedCipherSuites()),
                  tlsServerConfig.getIncludeCipherSuites(),
                  tlsServerConfig.getExcludeCipherSuites()
              );
            }
            if (sslEngine.getEnabledProtocols() == null || sslEngine.getEnabledProtocols().length == 0) {
              throw new ISE(
                  "No supported protocols found, supported protocols [%s], configured protocols include list: [%s] exclude list: [%s]",
                  Arrays.toString(sslEngine.getSupportedProtocols()),
                  tlsServerConfig.getIncludeProtocols(),
                  tlsServerConfig.getExcludeProtocols()
              );
            }
          }
        }

        @Override
        public void stop()
        {
          try {
            final long unannounceDelay = config.getUnannouncePropagationDelay().toStandardDuration().getMillis();
            if (unannounceDelay > 0) {
              log.info("Sleeping %s ms for unannouncement to propagate.", unannounceDelay);
              Thread.sleep(unannounceDelay);
            } else {
              log.debug("Skipping unannounce wait.");
            }
            log.debug("Stopping Jetty Server...");
            server.stop();
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RE(e, "Interrupted waiting for jetty shutdown.");
          }
          catch (Exception e) {
            log.warn(e, "Unable to stop Jetty server.");
          }
        }
      }, Lifecycle.Stage.SERVER);

    }

    private void buildErrorHandler()
    {
      if (config.isShowDetailedJettyErrors()) {
        return;
      }
      server.setErrorHandler(new ErrorHandler() {
        @Override
        public boolean isShowServlet()
        {
          return false;
        }

        @Override
        public void handle(
            String target,
            Request baseRequest,
            HttpServletRequest request,
            HttpServletResponse response
        ) throws IOException, ServletException
        {
          request.setAttribute(RequestDispatcher.ERROR_EXCEPTION, null);
          super.handle(target, baseRequest, request, response);
        }
      });
    }

    public Server build()
    {
      server = new Server(buildThreadPool());

      // Without this bean set, the default ScheduledExecutorScheduler runs as non-daemon, causing lifecycle hooks to fail
      // to fire on main exit. Related bug: https://github.com/apache/druid/pull/1627
      server.addBean(new ScheduledExecutorScheduler("JettyScheduler", true), true);

      buildConnectors();

      final long gracefulStop = config.getGracefulShutdownTimeout().toStandardDuration().getMillis();
      if (gracefulStop > 0) {
        server.setStopTimeout(gracefulStop);
      }
      buildLifecycleListener();

      // initialize server
      JettyServerInitializer initializer = injector.getInstance(JettyServerInitializer.class);
      try {
        initializer.initialize(server, injector);
      }
      catch (Exception e) {
        throw new RE(e, "server initialization exception");
      }

      buildLifecyleHandler();
      buildErrorHandler();

      return server;
    }
  }

  @Provides
  @Singleton
  public JettyMonitor getJettyMonitor(DataSourceTaskIdHolder dataSourceTaskIdHolder)
  {
    return new JettyMonitor(dataSourceTaskIdHolder.getDataSource(), dataSourceTaskIdHolder.getTaskId());
  }

  public static class JettyMonitor extends AbstractMonitor
  {
    private final Map<String, String[]> dimensions;

    public JettyMonitor(String dataSource, String taskId)
    {
      this.dimensions = MonitorsConfig.mapOfDatasourceAndTaskID(dataSource, taskId);
    }

    @Override
    public boolean doMonitor(ServiceEmitter emitter)
    {
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);
      emitter.emit(builder.build("jetty/numOpenConnections", ACTIVE_CONNECTIONS.get()));
      if (jettyServerThreadPool != null) {
        emitter.emit(builder.build("jetty/threadPool/total", jettyServerThreadPool.getThreads()));
        emitter.emit(builder.build("jetty/threadPool/idle", jettyServerThreadPool.getIdleThreads()));
        emitter.emit(builder.build("jetty/threadPool/isLowOnThreads", jettyServerThreadPool.isLowOnThreads() ? 1 : 0));
        emitter.emit(builder.build("jetty/threadPool/min", jettyServerThreadPool.getMinThreads()));
        emitter.emit(builder.build("jetty/threadPool/max", jettyServerThreadPool.getMaxThreads()));
        emitter.emit(builder.build("jetty/threadPool/queueSize", jettyServerThreadPool.getQueueSize()));
        emitter.emit(builder.build("jetty/threadPool/busy", jettyServerThreadPool.getBusyThreads()));
      }

      return true;
    }
  }

  private static class IdentityCheckOverrideSslContextFactory extends SslContextFactory.Server
  {
    private final TLSServerConfig tlsServerConfig;
    private final TLSCertificateChecker certificateChecker;

    public IdentityCheckOverrideSslContextFactory(
        TLSServerConfig tlsServerConfig,
        TLSCertificateChecker certificateChecker
    )
    {
      super();
      this.tlsServerConfig = tlsServerConfig;
      this.certificateChecker = certificateChecker;
    }

    @Override
    protected TrustManager[] getTrustManagers(
        KeyStore trustStore,
        Collection<? extends CRL> crls
    ) throws Exception
    {
      TrustManager[] trustManagers = super.getTrustManagers(trustStore, crls);
      TrustManager[] newTrustManagers = new TrustManager[trustManagers.length];

      for (int i = 0; i < trustManagers.length; i++) {
        if (trustManagers[i] instanceof X509ExtendedTrustManager) {
          newTrustManagers[i] = new CustomCheckX509TrustManager(
              (X509ExtendedTrustManager) trustManagers[i],
              certificateChecker,
              tlsServerConfig.isValidateHostnames()
          );
        } else {
          newTrustManagers[i] = trustManagers[i];
          log.info("Encountered non-X509ExtendedTrustManager: " + trustManagers[i].getClass());
        }
      }

      return newTrustManagers;
    }
  }

  @VisibleForTesting
  public int getActiveConnections()
  {
    return ACTIVE_CONNECTIONS.get();
  }

  @VisibleForTesting
  public static void setJettyServerThreadPool(QueuedThreadPool threadPool)
  {
    jettyServerThreadPool = threadPool;
  }
}
