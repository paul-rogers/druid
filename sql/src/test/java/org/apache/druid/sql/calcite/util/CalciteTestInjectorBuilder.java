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

package org.apache.druid.sql.calcite.util;

import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.math.expr.ExpressionProcessingConfig;
import org.apache.druid.sql.calcite.aggregation.SqlAggregationModule;
import org.apache.druid.sql.calcite.util.MockComponents.MockComponentsModule;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Create the injector used for {@link CalciteTests#INJECTOR}, but in a way
 * that is extensible.
 * <p>
 * The builder accepts properties to use when creating property objects,
 * as well as modules to use when building the injector. A number of standard
 * options are available as methods.
 * <p>
 * Unlike other injector builders, this one is "deferred": the base
 * injector builder is built at build time so that it can accept custom
 * properties provided here. Tests don't use properties from files.
 */
public class CalciteTestInjectorBuilder
{
  private Properties properties = new Properties(System.getProperties());
  private List<Module> modules = new ArrayList<>();
  private boolean withStatics;

  public CalciteTestInjectorBuilder()
  {
    add(
        new MockModules.BasicTestModule(),
        // For the ExprMacroTable dependency for PlannerFactory
        new ExpressionModule(),
        new MockModules.MockLookupSerdeModule()
    );
  }

  public CalciteTestInjectorBuilder withMockComponents()
  {
    add(new MockComponentsModule());
    //
    // add(new CalcitePlannerModule());
    return this;
  }

  /**
   * Default injector with the standard aggregates. Sketch tests
   * replace some of the standards with custom versions. To add
   * custom modules, while using the standard aggregates, include
   * the standard aggregate module as well.
   */
  public CalciteTestInjectorBuilder withSqlAggregation()
  {
    modules.add(new SqlAggregationModule());
    return this;
  }

  /**
   * Modules used by tests which derive from {@link CalciteTestBase}.
   */
  public CalciteTestInjectorBuilder forCalciteTests()
  {
    add(
        new MockModules.CalciteQueryTestModule(),
        new MockModules.MockSqlIngestionModule()
    );
    return this;
  }

  /**
   * Most tests expect to initialize null handling and expression
   * processing via static calls. Those that use properties (the preferred
   * approach) call this method to add the relevant modules to Guice.
   * With this enabled, the Guice-provided instances will overwrite those
   * defined statically, providing a migration path. However, if code
   * calls the null handling or expression processing setup methods
   * after Guice initialization, then those take precedence.
   * <p>
   * If statics are enabled, then null handling gets its value from
   * properties, which start with system properties. The result is that,
   * if statics are enabled, null handling is still controlled by the same
   * system property as when not enabled. The only difference is that the
   * code can override the property using {@link #property(String, Object)}.
   */
  public CalciteTestInjectorBuilder withStatics()
  {
    withStatics = true;
    return this;
  }

  public CalciteTestInjectorBuilder property(String key, Object value)
  {
    this.properties.setProperty(key, value.toString());
    return this;
  }

  public CalciteTestInjectorBuilder strictBooleans(boolean flag)
  {
    withStatics();
    property(ExpressionProcessingConfig.NULL_HANDLING_LEGACY_LOGICAL_OPS_KEY, flag);
    return this;
  }

  public CalciteTestInjectorBuilder add(Module...modules)
  {
    for (Module m : modules) {
      this.modules.add(m);
    }
    return this;
  }

  public CalciteTestInjectorBuilder addAll(List<Module> modules)
  {
    this.modules.addAll(modules);
    return this;
  }

  public Injector build()
  {
    try {
      StartupInjectorBuilder startupBuilder = new StartupInjectorBuilder()
          .withProperties(properties);
      if (withStatics) {
        startupBuilder.withStatics();
      }
      return new CoreInjectorBuilder(startupBuilder.build())
          .addAll(modules)
          .build();
    }
    catch (Exception e) {
      // Catches failures when used as a static initializer.
      e.printStackTrace();
      System.exit(1);
      throw e;
    }
  }
}
