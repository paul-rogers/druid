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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.tester.PlannerTestFixtureBuilder;
import org.apache.druid.sql.calcite.tester.QueryTestSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Test runner for query planner tests defined in ".case" files.
 * If the test fails, the test itself won't report many details.
 * Instead, look in target/actual for the "actual" files for failed
 * test. Diff them with the cases in test/resources/calcite/cases
 * to determine what changed.
 * <p>
 * Planner setup is mostly handled by the {@code PlannerFixture}
 * class, with some additional test-specific configuration
 * applied for each group of test (each case file or set of case
 * files).
 * <p>
 * All tests use the set of hard-coded, in-memory segments defined
 * by {@code CalciteTests}. Tests can optionally include lookups
 * and views, if required for those tests.
 */
public class DruidPlannerTest
{
  public static final Logger log = new Logger(DruidPlannerTest.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private PlannerTestFixtureBuilder standardBuilder() throws IOException
  {
    return new PlannerTestFixtureBuilder()
        .standardComponents(temporaryFolder.newFolder())
        .defaultQueryContext(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT);
  }

  // Converted from CalciteParameterQueryTest
  @Test
  public void testParameterQuery() throws IOException
  {
    assertTrue(QueryTestSet
        .fromResource("/calcite/cases/parameterQuery.case")
        .run(standardBuilder()));
  }
}
