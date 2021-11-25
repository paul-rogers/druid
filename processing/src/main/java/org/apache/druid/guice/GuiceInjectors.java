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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import io.netty.util.SuppressForbidden;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExpressionProcessingModule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

/**
 */
public class GuiceInjectors
{
  public static Collection<Module> makeRootModules()
  {
    return ImmutableList.of(
        new DruidGuiceExtensions(),
        new PropertiesModule(Arrays.asList("common.runtime.properties", "runtime.properties"))
    );
  }

  public static Collection<Module> makeDefaultStartupModules()
  {
    return ImmutableList.of(
        // The following (mostly) configure Jackson.
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

  public static String guiceMap(Injector injector)
  {
    StringBuilder buf = new StringBuilder();
    if (injector.getParent() != null) {
      buf.append("-- Parent --\n");
      buf.append(guiceMap(injector.getParent()));
      buf.append("----\n");
    }
    for (Entry<Key<?>, Binding<?>> entry : injector.getBindings().entrySet()) {
      buf.append(StringUtils.format("%s: %s\n",
          entry.getKey().toString(),
          entryToString(entry.getValue())));
    }
    return buf.toString();
  }

  public static String entryToString(Binding<?> binding)
  {
    StringBuilder buf = new StringBuilder();
    buf.append(binding.getClass().getSimpleName());
    buf.append("(");
    Provider<?> provider = binding.getProvider();
    if (provider == null) {
      buf.append("null");
    } else {
      buf.append(provider.getClass().getSimpleName());
    }
    buf.append(")");
    return buf.toString();
  }

  @VisibleForTesting
  @SuppressForbidden(reason = "System#out")
  public static void printMap(Injector injector)
  {
    System.out.println("Guice map");
    System.out.print(guiceMap(injector));
  }
}
