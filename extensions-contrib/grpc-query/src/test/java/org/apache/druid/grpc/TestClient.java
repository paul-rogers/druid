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

package org.apache.druid.grpc;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.druid.grpc.proto.QueryGrpc;
import org.apache.druid.grpc.proto.QueryGrpc.QueryBlockingStub;

import java.util.concurrent.TimeUnit;

/**
 * Super-simple test client that connects to a gRPC query endpoint
 * and allows submitting a rRPC query request and returns the response.
 * The server can be in the same or another process.
 */
public class TestClient
{
  private ManagedChannel channel;
  private QueryBlockingStub client;

  public TestClient()
  {
    // Access a service running on the local machine on port 50051
    //this("localhost:50051");
    this("localhost:8082");
  }

  public TestClient(String target)
  {
    // Create a communication channel to the server, known as a Channel. Channels are thread-safe
    // and reusable. It is common to create channels at the beginning of your application and reuse
    // them until the application shuts down.
    //
    // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
    // use TLS, use TlsChannelCredentials instead.
    channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
        .build();
    client = QueryGrpc.newBlockingStub(channel);
  }

  public QueryBlockingStub client()
  {
    return client;
  }

  public void close() throws InterruptedException
  {
    // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
    // resources the channel should be shut down when it will no longer be used. If it may be used
    // again leave it running.
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
  }
}
