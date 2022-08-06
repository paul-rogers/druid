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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.guice.SqlBindings;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.List;

/**
 * The Calcite tests are configured using Guice, but often we want a
 * different configuration of modules than used in production. This class
 * holds a collection of such "test-only" modules. In some cases we omit
 * unwanted dependencies (such a on the web server). In other cases, we
 * substitute test-only versions of components.
 * <p>
 * A future improvement is to use the actual production modules, especially
 * when the module provides an extension mechanism: we set up the required
 * properties to point to the test versions. In other cases, perhaps we can
 * split the modules into a "core" and an "REST" version so tests can
 * depend on just the core. The ultimate goal is for this class to become
 * empty and disappear.
 * <p>
 * The modules here are for standard items used by all Calcite test. Each
 * test can add its own modules, as is common for aggregation and sketch
 * tests.
 */
public class MockModules
{
  /**
   * Test-only version of {@link org.apache.druid.query.lookup.LookupSerdeModule
   * LookupSerdeModule}. Omits the binding to {@code
   * LookupExtractorFactoryContainerProvider} as that is done later with
   * test-specific classes to enable the "lookyloo" lookup.
   * <p>
   * A future improvement is to use the actual {@code LookupSerdeModule},
   * and use test-specific properties to bind to the test lookup setup.
   * Doing so would validate the property mechanism as well.
   */
  static class MockLookupSerdeModule implements DruidModule
  {
    @Override
    public void configure(Binder binder)
    {
      final LookupExtractorFactoryContainerProvider lookupProvider =
          LookupEnabledTestExprMacroTable.createTestLookupProvider(
              ImmutableMap.of(
                  "a", "xa",
                  "abc", "xabc",
                  "nosuchkey", "mysteryvalue",
                  "6", "x6"
              )
          );

      // This Module is just to get a LookupExtractorFactoryContainerProvider with a usable "lookyloo" lookup.
      binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupProvider);
      ExpressionModule.addExprMacro(binder, LookupExprMacro.class);
    }

    @Override
    public List<? extends Module> getJacksonModules()
    {
      return new LookupSerdeModule().getJacksonModules();
    }
  }

  /**
   * Random collection of Guice bindings needed for tests.
   * <p>
   * A future improvement is to replace this module with either the actual
   * modules, or "test" versions of the actual modules.
   */
  static class BasicTestModule implements DruidModule
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
      SqlBindings.addOperatorConversion(binder, QueryLookupOperatorConversion.class);
    }
  }

  /**
   * Dependencies for the SQL ingestion (INSERT, REPLACE) functionality.
   * Revisit once the multi-stage components are fully merged into Druid.
   */
  public static class MockSqlIngestionModule implements DruidModule
  {
    @Override
    public void configure(Binder binder)
    {
      // Add "EXTERN" table macro, for CalciteInsertDmlTest.
      SqlBindings.addOperatorConversion(binder, ExternalOperatorConversion.class);
    }

    @Override
    public List<? extends Module> getJacksonModules()
    {
      final List<Module> modules = new ArrayList<>();
      modules.add(new SimpleModule().registerSubtypes(ExternalDataSource.class));
      return modules;
    }
  }

  public static class CalciteQueryTestModule implements DruidModule
  {
    @Override
    public void configure(Binder binder)
    {
      //    binder.bind(ViewManager.class).to(CalciteTestsViewManager.class).in(LazySingleton.class);
    }
  }
}
