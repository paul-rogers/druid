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
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.servlet.GuiceFilter;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationUtils;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.List;

/**
 */
public interface JettyServerInitializer
{
  void initialize(Server server, Injector injector);

  /**
   * Builder for Druid services. Standardizes the required steps, while allowing
   * each service to insert additional initialization as needed.
   */
  public static class Builder
  {
    private enum State
    {
      START, AUTH, HANDLERS, DONE
    }
    private final Server server;
    private final ServerConfig serverConfig;
    private final Injector injector;
    private final ServletContextHandler root;
    private final HandlerList handlerList = new HandlerList();
    private final ObjectMapper jsonMapper;
    private final AuthConfig authConfig;
    private Handler rootHandler = handlerList;
    private State state = State.START;

    public Builder(Server server, ServerConfig serverConfig, Injector injector)
    {
      this.server = server;
      this.serverConfig = serverConfig;
      this.injector = injector;
      this.jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
      this.authConfig = injector.getInstance(AuthConfig.class);
      this.root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    }

    public Builder withDefaultServlet()
    {
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      return this;
    }

    public Builder withServletHolder()
    {
      root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
      ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);
      root.addServlet(holderPwd, "/");
      return this;
    }

    public Builder startAuth()
    {
      if (state != State.START) {
        throw new ISE("startAuth() already called.");
      }
      state = State.AUTH;

      AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);
      unsecuredPaths(authConfig.getUnsecuredPaths());
      return this;
    }

    public Builder endAuth()
    {
      if (state != State.AUTH) {
        throw new ISE("call startAuth() first.");
      }
      state = State.HANDLERS;

      final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

      List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
      AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

      AuthenticationUtils.addAllowOptionsFilter(root, authConfig.isAllowUnauthenticatedHttpOptions());
      JettyServerInitUtils.addAllowHttpMethodsFilter(root, serverConfig.getAllowedHttpMethods());

      JettyServerInitUtils.addExtensionFilters(root, injector);

      // Check that requests were authorized before sending responses
      AuthenticationUtils.addPreResponseAuthorizationCheckFilter(
          root,
          authenticators,
          jsonMapper
      );
      return this;
    }

    public Builder unsecuredPaths(List<String> paths)
    {
      if (state != State.AUTH) {
        throw new ISE("Call startAuth() first.");
      }
      // perform no-op authorization for these resources
      AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, paths);
      return this;
    }

    public Builder guicePath(String path)
    {
      root.addFilter(GuiceFilter.class, path, null);
      return this;
    }

    public Builder guicePaths(String[] paths)
    {
      for (String path : paths) {
        guicePath(path);
      }
      return this;
    }

    // TODO: Needed? Used for query resource
    public Builder keepServerHandlers()
    {
      // Do not change the order of the handlers that have already been added
      for (Handler handler : server.getHandlers()) {
        handlerList.addHandler(handler);
      }
      return this;
    }

    public Builder addHandler(Handler handler)
    {
      handlerList.addHandler(handler);
      return this;
    }

    public Builder addHandlers(Iterable<Handler> handlers)
    {
      for (Handler handler : handlers) {
        handlerList.addHandler(handler);
      }
      return this;
    }

    public Builder standardHandlers()
    {
      handlerList.addHandler(JettyServerInitUtils.getJettyRequestLogHandler());

      // Add Gzip handler at the very end
      handlerList.addHandler(JettyServerInitUtils.wrapWithDefaultGzipHandler(
          root,
          serverConfig.getInflateBufferSize(),
          serverConfig.getCompressionLevel()));
      return this;
    }

    public Builder withStatistics()
    {
      final StatisticsHandler statisticsHandler = new StatisticsHandler();
      statisticsHandler.setHandler(handlerList);
      rootHandler = statisticsHandler;
      return this;
    }

    public void build()
    {
      if (state != State.HANDLERS) {
        throw new ISE("Call endAuth() first.");
      }
      state = State.DONE;
      server.setHandler(rootHandler);
    }

    public ServletContextHandler root()
    {
      return root;
    }
  }
}
