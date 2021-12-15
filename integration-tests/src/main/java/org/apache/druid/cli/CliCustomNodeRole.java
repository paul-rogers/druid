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

package org.apache.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.airline.Command;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.Services;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.SelfDiscoveryResource;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.util.List;

@Command(
    name = CliCustomNodeRole.SERVICE_NAME,
    description = "Some custom druid node role defined in an extension"
)
public class CliCustomNodeRole extends ServerRunnable
{
  private static final Logger LOG = new Logger(CliCustomNodeRole.class);

  public static final String SERVICE_NAME = "custom-node-role";
  public static final int PORT = 9301;
  public static final int TLS_PORT = 9501;

  public CliCustomNodeRole()
  {
    super(LOG);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        binder -> {
          LOG.info("starting up");
          Services.bindService(
              binder,
              CliCustomNodeRole.SERVICE_NAME,
              CliCustomNodeRole.PORT,
              CliCustomNodeRole.TLS_PORT);

          binder.bind(CoordinatorClient.class).in(LazySingleton.class);

          binder.bind(JettyServerInitializer.class).to(CustomJettyServiceInitializer.class).in(LazySingleton.class);
          LifecycleModule.register(binder, Server.class);

          bindNodeRoleAndAnnouncer(
              binder,
              DiscoverySideEffectsProvider.builder(new NodeRole(CliCustomNodeRole.SERVICE_NAME)).build()
          );
          Jerseys.addResource(binder, SelfDiscoveryResource.class);
          LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));

        }
    );
  }

  // Mimic of other Jetty initializers
  private static class CustomJettyServiceInitializer implements JettyServerInitializer
  {
    private static List<String> UNSECURED_PATHS = ImmutableList.of(
        "/status/health"
    );

    private final ServerConfig serverConfig;

    @Inject
    public CustomJettyServiceInitializer(ServerConfig serverConfig)
    {
      this.serverConfig = serverConfig;
    }

    @Override
    public void initialize(Server server, Injector injector)
    {
      new Builder(server, serverConfig, injector)
          .withDefaultServlet()
          .startAuth()
          // The builder adds unsecuredPaths(authConfig.getUnsecuredPaths());
          // But this original code did not. OK?
          .unsecuredPaths(UNSECURED_PATHS)
          .endAuth()
          .guicePath("/*")
          .keepServerHandlers()
          .withStatistics()
          .build();
    }
  }
}
