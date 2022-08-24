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

package org.apache.druid.sql.calcite.tester;

import com.google.inject.Injector;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.util.CalciteTestInjectorBuilder;
import org.apache.druid.sql.calcite.util.QueryFramework;

import java.io.File;
import java.util.Map;

public class PlannerTestFixtureBuilder
{
  private final Injector injector;
  private QueryFramework.Builder frameworkBuilder;
  private PlannerTestFixture.Builder fixtureBuilder = new PlannerTestFixture.Builder();

  public PlannerTestFixtureBuilder()
  {
    injector = new CalciteTestInjectorBuilder().build();
  }

  public Injector injector()
  {
    return injector;
  }

  public PlannerTestFixtureBuilder standardComponents(final File temporaryFolder)
  {
    frameworkBuilder = new QueryFramework.Builder(
        new QueryFramework.StandardComponentSupplier(
            injector,
            temporaryFolder
        )
    );
    frameworkBuilder.injector(injector);
    return this;
  }

  public PlannerTestFixtureBuilder withAuthResult(AuthenticationResult authResult)
  {
    fixtureBuilder.withAuthResult(authResult);
    return this;
  }

  /**
   * Define the query context to use for running the query.
   * Also used for planning, if the planning context is not set.
   *
   * @see #defaultQueryPlanningContext()
   */
  public PlannerTestFixtureBuilder defaultQueryContext(Map<String, Object> context)
  {
    fixtureBuilder.defaultQueryContext(context);
    return this;
  }

  public QueryFramework.Builder frameworkBuilder()
  {
    return frameworkBuilder;
  }

  public QueryFramework buildFramework()
  {
    return frameworkBuilder.build();
  }

  public PlannerTestFixture.Builder fixtureBuilder()
  {
    return fixtureBuilder;
  }

  public PlannerTestFixture buildFixture()
  {
    return fixtureBuilder.build(buildFramework());
  }
}
