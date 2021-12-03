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

package org.apache.druid.guice;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.math.expr.ExpressionProcessingModule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
public class GuiceInjectors
{
  public static Collection<Module> makeRootModules()
  {
    return ImmutableList.of(
        new DruidGuiceExtensions(),
        new PropertiesModule(Arrays.asList("common.runtime.properties", "runtime.properties")),
        new ConfigModule.EarlyConfigModule()
    );
  }

  public static Collection<Module> makeDefaultStartupModules()
  {
    return ImmutableList.of(
        // The following (mostly) configures Jackson.
        // TODO: Move to the root injector and remove from DruidSecondaryModule
        // Requires resolving some mysteries, however.
        new JacksonModule(),
        new RuntimeInfoModule(),
        new ConfigModule(),
        new NullHandlingModule(),
        new ExpressionProcessingModule(),
        binder -> {
          binder.bind(DruidSecondaryModule.class);
          JsonConfigProvider.bind(binder, "druid.extensions", ExtensionsConfig.class);
          JsonConfigProvider.bind(binder, "druid.modules", ModulesConfig.class);
        }
    );
  }

  public static Injector makeStartupInjector()
  {
    final Injector root = Guice.createInjector(makeRootModules());
//    Tools.printMap(root);
    return root.createChildInjector(makeDefaultStartupModules());
  }

  public static Injector makeStartupInjectorWithModules(Iterable<? extends Module> modules)
  {
    List<Module> theModules = new ArrayList<>(makeDefaultStartupModules());
    for (Module theModule : modules) {
      theModules.add(theModule);
    }
    return Guice.createInjector(theModules);
  }

  public static final Named SERVICE_NAME = Names.named("serviceName");
  public static final Named SERVICE_PORT = Names.named("servicePort");
  public static final Named SERVICE_TLS_PORT = Names.named("tlsServicePort");
  public static final int NULL_TLS_PORT = -1;

  /**
   * Define an internal service with no external ports.
   */
  public static void bindService(Binder binder, String serviceName)
  {
    bindService(binder, serviceName, 0, NULL_TLS_PORT);
  }

  /**
   * Define a service which exposes ports.
   */
  public static void bindService(Binder binder, String serviceName, int servicePort, int tlsServicePort)
  {
    binder.bindConstant().annotatedWith(SERVICE_NAME).to(serviceName);
    binder.bindConstant().annotatedWith(SERVICE_PORT).to(servicePort);
    binder.bindConstant().annotatedWith(SERVICE_TLS_PORT).to(tlsServicePort);
  }
}
