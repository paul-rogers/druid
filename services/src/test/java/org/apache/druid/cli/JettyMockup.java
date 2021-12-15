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

import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

public class JettyMockup
{
  public static class MyHandler extends AbstractHandler
  {
    private final String msg;

    public MyHandler()
    {
      this("Hello World");
    }

    public MyHandler(String msg)
    {
      this.msg = msg;
    }

    @Override
    public void handle(String target,
        Request baseRequest,
        HttpServletRequest request,
        HttpServletResponse response) throws IOException,
        ServletException
    {
      // Declare response encoding and types
      response.setContentType("text/html; charset=utf-8");

      // Declare response status code
      response.setStatus(HttpServletResponse.SC_OK);

      // Write back response
      response.getWriter().println("<h1>" + msg + "</h1>");

      // Inform jetty that this request has now been handled
      baseRequest.setHandled(true);
    }
  }

  @SuppressWarnings("serial")
  public static class HelloServlet extends HttpServlet
  {
    @Override
    protected void doGet(HttpServletRequest request,
        HttpServletResponse response) throws IOException
    {
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("text/html");
      response.setCharacterEncoding("utf-8");
      response.getWriter().println("<h1>Hello from HelloServlet</h1>");
    }
  }

  public static class TestFilter implements Filter
  {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException
    {
      System.out.println("Hit");
      chain.doFilter(request, response);
    }

    @Override
    public void destroy()
    {
    }
  }

  public static Server createServer() throws Exception
  {
    Server server = new Server();

    ServerConnector http1 = new ServerConnector(server);
    http1.setHost("localhost");
    http1.setPort(9998);
    http1.setIdleTimeout(30000);
    http1.setName("http1");

    ServerConnector http2 = new ServerConnector(server);
    http2.setHost("localhost");
    http2.setPort(9999);
    http2.setIdleTimeout(30000);
    http2.setName("http2");

    // Set the connector
    server.setConnectors(new Connector[] {
        http1,
        http2
        });

    ServletContextHandler context1 = new ServletContextHandler();
    context1.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    //    context1.setContextPath("/*");
    context1.addServlet(HelloServlet.class, "/hello");
    //    context1.setHandler(new MyHandler());
    context1.setVirtualHosts(new String[] {"@http1"});

    ServletContextHandler context2 = new ServletContextHandler();
    //    context2.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    //    context2.setContextPath("/*");
    //    context2.setHandler(new MyHandler("Hola Mundo"));
    context2.setVirtualHosts(new String[] {"@http2"});
    context2.addFilter(new FilterHolder(new TestFilter()), "/*", null);

    ServletHolder jerseyServlet = context2.addServlet(
        ServletContainer.class,
        "/*"
        );
    jerseyServlet.setInitOrder(0);

    // Tells the Jersey Servlet which REST service/class to load.
    jerseyServlet.setInitParameter(
        "javax.ws.rs.Application",
        ClassNamesResourceConfig.class.getCanonicalName());
    jerseyServlet.setInitParameter(
        "com.sun.jersey.config.property.classnames",
        TestResource.class.getCanonicalName());

    ContextHandlerCollection contexts = new ContextHandlerCollection(
        context1,
        context2
    );

    server.setHandler(contexts);
    //    server.setHandler(new MyHandler());

    //    ServletHandler handler = new ServletHandler();
    //    server.setHandler(handler);
    //    handler.addServletWithMapping(HelloServlet.class, "/*");

    return server;
  }

  public static void main(String[] args) throws Exception
  {
    Server server = createServer();
    server.start();
    server.join();
  }
}
