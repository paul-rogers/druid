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
import org.apache.druid.guice.DruidProcessingConfigModule;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExpressionProcessingConfig;
import org.apache.druid.sql.calcite.aggregation.SqlAggregationModule;
import org.apache.druid.sql.calcite.util.MockComponents.MockComponentsModule;
import org.apache.druid.sql.calcite.util.MockModules.CalciteQueryTestModule;
import org.apache.druid.sql.calcite.util.MockModules.MockQueryableModule;

import java.io.IOException;
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
 * Modules added here can implement {@link ManagedDruidModule} to provide a
 * simple way to initialize test data that depends on objects within the
 * injector. The injector holds a {@link Closer} so that tests need not
 * handle that. The {@code Closer} is closed in the {@link #tearDown(Injector)}
 * method.
 * <p>
 * Unlike other injector builders, this one is "deferred": the base
 * injector builder is built at build time so that it can accept custom
 * properties provided here. Tests don't use properties from files.
 */
public class CalciteTestInjectorBuilder
{
  /**
   * Many test components are mocks that are initialized in-process from mock
   * data. Normal Guice modules assume initialization occurs in a constructor,
   * but this results in awkward dependencies in tests. Druid provides a
   * lifecycle manager, but that is overkill in tests. This interface is a
   * happy medium. Modules are initialized in the order in which they are added
   * to the builder and torn down in opposite order.
   */
  public interface ManagedDruidModule extends DruidModule
  {
    default void setup(Injector injector)
    {
    }

    default void teardown(Injector injector)
    {
    }
  }

  private Properties properties = new Properties(System.getProperties());
  private List<Module> modules = new ArrayList<>();
  private boolean withStatics;
  private boolean omitSqlAggregation;

  public CalciteTestInjectorBuilder()
  {
    add(
        new MockModules.BasicTestModule(),
        // For the ExprMacroTable dependency for PlannerFactory
        new ExpressionModule(),
        new MockModules.MockLookupSerdeModule()
//        binder -> {
//          binder.bind(Closer.class).in(LazySingleton.class);
//        }
    );
  }

  public CalciteTestInjectorBuilder withMockComponents()
  {
    add(
        // For DruidProcessingConfig, needed by the mock conglomerate.
        // Configure using injector properties.
//        new DruidProcessingConfigModule(),
        new MockComponentsModule(),
        new MockQueryableModule(),
        new CalciteQueryTestModule()
    );
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
  public CalciteTestInjectorBuilder omitSqlAggregation()
  {
    omitSqlAggregation = true;
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
      if (!omitSqlAggregation) {
        modules.add(new SqlAggregationModule());
      }
      StartupInjectorBuilder startupBuilder = new StartupInjectorBuilder()
          .withProperties(properties);
      if (withStatics) {
        startupBuilder.withStatics();
      }
      Injector injector = new CoreInjectorBuilder(startupBuilder.build())
          .addAll(modules)
          .build();
      for (Module module : modules) {
        if (module instanceof ManagedDruidModule) {
          ((ManagedDruidModule) module).setup(injector);
        }
      }
      return injector;
    }
    catch (Exception e) {
      // Catches failures when used as a static initializer.
      e.printStackTrace();
      System.exit(1);
      throw e;
    }
  }

  public void tearDown(Injector injector)
  {
    for (Module module : modules) {
      if (module instanceof ManagedDruidModule) {
        ((ManagedDruidModule) module).teardown(injector);
      }
    }
    try {
      injector.getInstance(Closer.class).close();
    } catch (IOException e) {
      throw new ISE(e, "Something failed to close");
    }
  }
}
