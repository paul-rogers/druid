package org.apache.druid.cli;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

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
    server.setConnectors(new Connector[] { http1, http2 } );

    ContextHandler context1 = new ContextHandler();
    context1.setContextPath("/hello");
    context1.setHandler(new MyHandler());
    context1.setVirtualHosts(new String[] { "http1" } );

    ContextHandler context2 = new ContextHandler();
    context2.setContextPath("/hello");
    context2.setHandler(new MyHandler("Hola Mundo"));
    context2.setVirtualHosts(new String[] { "http2" } );

    ContextHandlerCollection contexts = new ContextHandlerCollection(
        context1 //, context2
    );

    server.setHandler(contexts);
    return server;
  }

  public static void main(String[] args) throws Exception
  {
    Server server = createServer();
    server.start();
    server.join();
  }

}
