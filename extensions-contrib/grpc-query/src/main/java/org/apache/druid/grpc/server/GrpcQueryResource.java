package org.apache.druid.grpc.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.servlet.ServletAdapter;
import io.grpc.servlet.ServletServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.druid.grpc.proto.QueryGrpc;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.NativeQuery;
import org.apache.druid.sql.SqlStatementFactory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

import java.io.IOException;

/**
 * Jersey resource which emulates a servlet which wraps an async stream which
 * wraps a gRPC server which implements the Druid gRPC query service which calls
 * a driver which calls the Druid SQL layer. Security is provided via Druid filters
 * which work with the servlet request. The request is piped around the gRPC servlet
 * async code into the Druid SQL layer. It ain't pretty, but it works.
 * <p>
 * Uses Druid's normal auth mechanisms. WARNING: if this code fails to mark the request
 * as authorized, the security filter will ignore the actual response and insert its
 * own. The form of that response causes the client to hang forever.
 *
 * @see <a href="https://github.com/grpc/grpc-java/tree/master/servlet/src/main/java/io/grpc/servlet">gRPC Sources</a>
 */
@Path("/druidGrpc.Query")
public class GrpcQueryResource
{
  private class QueryService extends QueryGrpc.QueryImplBase
  {
    private final HttpServletRequest servletRequest;

    private QueryService(final HttpServletRequest servletRequest)
    {
      this.servletRequest = servletRequest;
    }

    @Override
    public void submitQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver)
    {
      QueryResponse reply = driver.submitQuery(request, servletRequest);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  private final QueryDriver driver;

  @Inject
  public GrpcQueryResource(
      final @Json ObjectMapper jsonMapper,
      final @NativeQuery SqlStatementFactory sqlStatementFactory
  )
  {
    this.driver = new QueryDriver(jsonMapper, sqlStatementFactory);
  }

  @POST
  @Path("SubmitQuery")
  public void doPost(
      @Context final HttpServletRequest request,
      @Context final HttpServletResponse response
  ) throws IOException
  {
    if (!ServletAdapter.isGrpc(request)) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Non-gRPC not supported");
      return;
    }
    new ServletServerBuilder()
        .addService(new QueryService(request))
        .buildServletAdapter()
        .doPost(request, response);
  }
}
