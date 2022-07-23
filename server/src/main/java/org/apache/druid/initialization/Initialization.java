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

package org.apache.druid.initialization;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * Initialize Guice for a server. Clients and tests should use
 * the individual builders to create a non-server environment.
 */
public class Initialization
{
  // Use individual builders for testing: this method brings in
  // server-only dependencies, which is generally not desired.
  @Deprecated
  public static Injector makeInjectorWithModules(
      final Injector baseInjector,
      final Iterable<? extends Module> modules
  )
  {
    return ServerInjectorBuilder.makeServerInjector(baseInjector, ImmutableSet.of(), modules);
  }
}
