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

import com.google.inject.Injector;
import io.airlift.airline.Cli;
import io.airlift.airline.ParseException;
import org.apache.druid.guice.GuiceInjectors;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests parsing of the "bundle" command that runs multiple Druid services
 * in one executable. Does not actually run the commands.
 */
public class BundleTest
{
  public Cli<Runnable> buildCli() {
    final Injector injector = GuiceInjectors.makeStartupInjector();
    return Main.buildCli(injector);
  }

  @Test(expected = ParseException.class)
  public void testNone()
  {
    Cli<Runnable> cli = buildCli();
    String args[] = new String[] {"bundle"};
    cli.parse(args).run();
  }

  @Test
  public void testOne()
  {
    Cli<Runnable> cli = buildCli();
    String args[] = new String[] {"bundle", "broker"};
    Runnable cmd = cli.parse(args);
    assertTrue(cmd instanceof BundleCommand);
    BundleCommand bundle = (BundleCommand) cmd;
    assertTrue(bundle.broker);
    assertFalse(bundle.all);
    assertFalse(bundle.historical);
  }

  @Test
  public void testTwo()
  {
    Cli<Runnable> cli = buildCli();
    String args[] = new String[] {"bundle", "broker", "historical"};
    Runnable cmd = cli.parse(args);
    assertTrue(cmd instanceof BundleCommand);
    BundleCommand bundle = (BundleCommand) cmd;
    assertTrue(bundle.broker);
    assertTrue(bundle.historical);
  }

  // Broker and router use the same port: can't run together.
  // OK, because there is no need for the router if we have a broker.
  @Test(expected = ParseException.class)
  public void testBrokerRouterConflict()
  {
    Cli<Runnable> cli = buildCli();
    String args[] = new String[] {"bundle", "broker", "router"};
    cli.parse(args).run();
  }
}
