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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.initialization.jetty.LimitRequestsFilter;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.List;
import java.util.Set;

/**
 *
 */
public class QueryJettyServerInitializer implements JettyServerInitializer
{
  private static final Logger log = new Logger(QueryJettyServerInitializer.class);
  private static List<String> UNSECURED_PATHS = Lists.newArrayList(
      "/status/health",
      "/druid/historical/v1/readiness",
      "/druid/broker/v1/readiness"
  );

  private final List<Handler> extensionHandlers;
  private final ServerConfig serverConfig;

  @Inject
  public QueryJettyServerInitializer(Set<Handler> extensionHandlers, ServerConfig serverConfig)
  {
    this.extensionHandlers = ImmutableList.copyOf(extensionHandlers);
    this.serverConfig = serverConfig;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final Builder builder = new Builder(server, serverConfig, injector)
        .withDefaultServlet();
    final ServletContextHandler root = builder.root();
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    // Add LimitRequestsFilter as first in the chain if enabled.
    if (serverConfig.isEnableRequestLimit()) {
      //To reject xth request, limit should be set to x-1 because (x+1)st request wouldn't reach filter
      // but rather wait on jetty queue.
      Preconditions.checkArgument(
          serverConfig.getNumThreads() > 1,
          "numThreads must be > 1 to enable Request Limit Filter."
      );
      log.info("Enabling Request Limit Filter with limit [%d].", serverConfig.getNumThreads() - 1);
      root.addFilter(new FilterHolder(new LimitRequestsFilter(serverConfig.getNumThreads() - 1)),
                     "/*", null
      );
    }

    builder
        .startAuth()
        .unsecuredPaths(UNSECURED_PATHS)
        .endAuth()
        .guicePath("/*")
        .keepServerHandlers()
        .addHandlers(extensionHandlers)
        // TODO: This places the Jetty request log handler after
        // the extensions. OK?
        .keepServerHandlers()
        .withStatistics()
        .build();
  }
}
