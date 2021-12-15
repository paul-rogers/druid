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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.ParseException;

/**
 * Runs multiple Druid services in a single Java executable.
 * <p>
 * Services are represented as options, with names and descriptions imported
 * from the single-service classes. Done this way because options have to be
 * member variables: there seems no dynamic way to define options, sadly.
 */
@Command(
    name = "bundle",
    description = "Experimental. Runs multiple Druid services. " +
        "Great for testing. Not for production use."
)
public class BundleCommand implements Runnable
{
  @Option(name = CliBroker.NAME, description = CliBroker.DESCRIPTION)
  public boolean broker;
  @Option(name = CliCoordinator.NAME, description = CliCoordinator.DESCRIPTION)
  public boolean coordinator;
  @Option(name = CliHistorical.NAME, description = CliHistorical.DESCRIPTION)
  public boolean historical;
  @Option(name = CliOverlord.NAME, description = CliOverlord.DESCRIPTION)
  public boolean overlord;
  @Option(name = CliIndexer.NAME, description = CliIndexer.DESCRIPTION)
  public boolean indexer;
  @Option(name = CliMiddleManager.NAME, description = CliMiddleManager.DESCRIPTION)
  public boolean middleManager;
  @Option(name = CliRouter.NAME, description = CliRouter.DESCRIPTION)
  public boolean router;
  @Option(name = "all", description = "Runs all services for a single-process Druid.")
  public boolean all;

  @Override
  public void run()
  {
    if (all) {
      broker = true;
      coordinator = true;
      historical = true;
      overlord = true;
      indexer = true;
      middleManager = true;
      router = true;
    }
    if (!broker && !coordinator && !historical && !overlord && !indexer &&
        !middleManager && !router) {
      throw new ParseException("Must specify at least one service.");
    }
    // Broker and router share a port. Use just one.
    if (broker && router) {
      throw new ParseException("Cannot run a router with a broker, router is redundant.");
    }
  }
}

