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
import org.apache.druid.server.http.OverlordProxyServlet;
import org.apache.druid.server.http.RedirectFilter;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.List;
import java.util.Properties;

/**
 */
class CoordinatorJettyServerInitializer implements JettyServerInitializer
{
  private static List<String> UNSECURED_PATHS = ImmutableList.of(
      "/coordinator/false",
      "/overlord/false",
      "/status/health",
      "/druid/coordinator/v1/isLeader"
  );

  private final boolean beOverlord;
  private final ServerConfig serverConfig;

  @Inject
  CoordinatorJettyServerInitializer(Properties properties, ServerConfig serverConfig)
  {
    this.beOverlord = CliCoordinator.isOverlord(properties);
    this.serverConfig = serverConfig;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final Builder builder = new Builder(server, serverConfig, injector)
        .withServletHolder()
        .startAuth()
        .unsecuredPaths(UNSECURED_PATHS);
    if (beOverlord) {
      builder.unsecuredPaths(CliOverlord.UNSECURED_PATHS);
    }

    final ServletContextHandler root = builder.root();
    WebConsoleJettyServerInitializer.intializeServerForWebConsoleRoot(root);
    builder.endAuth();

    // redirect anything other than status to the current lead
    root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);
    builder.guicePaths(new String[] {
        // add some paths not to be redirected to leader.
        "/status/*",
        "/druid-internal/*",


        // The coordinator really needs a standardized API path
        // Can't use '/*' here because of Guice and Jetty static content conflicts
        "/info/*",
        "/druid/coordinator/*",
        "/druid-ext/*",

        // this will be removed in the next major release
        "/coordinator/*",
        });

    if (beOverlord) {
      builder.guicePath("/druid/indexer/*");
    } else {
      root.addServlet(new ServletHolder(injector.getInstance(OverlordProxyServlet.class)), "/druid/indexer/*");
    }

    builder
        .addHandler(WebConsoleJettyServerInitializer.createWebConsoleRewriteHandler())
        .standardHandlers()
        .build();
  }
}
