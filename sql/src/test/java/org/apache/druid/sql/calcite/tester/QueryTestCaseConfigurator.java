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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.ExpressionProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.planner.PlannerConfig;

/**
 * Runs a test case and captures the planning-related aspects
 * of the query that the test case says to verify.
 * <p>
 * Druid is irritating in that several options are global, yet tests
 * want to test variations. This appears to normally be done by running
 * tests with different command-line settings, which is clunky. We want
 * to set those options in-line. Further, some of the the global options
 * are cached in the planner, forcing us to rebuild the entire planner
 * when the options change. This is clearly an opportunity for improvement.
 */
public class QueryTestCaseConfigurator
{
  public static final Logger log = new Logger(QueryTestCaseConfigurator.class);

  private final PlannerTestFixture baseFixture;
  private final QueryTestCase testCase;

  public QueryTestCaseConfigurator(PlannerTestFixture plannerFixture, QueryTestCase testCase)
  {
    this.baseFixture = plannerFixture;
    this.testCase = testCase;
  }

  public ActualResults run()
  {
    return runWithExpressionOptions();
  }

  private ActualResults runWithExpressionOptions()
  {
    // Horrible, hacky way to change the way Druid handles
    // expressions. The config is meant to be global, initialized
    // on startup. This is a cheap workaround.
    // Only works for single-threaded tests.
    boolean allowNestedArrays = testCase.booleanOption(OptionsSection.ALLOW_NESTED_ARRAYS);
    boolean homogenizeNullMultiValueStrings = testCase.booleanOption(OptionsSection.HOMOGENIZE_NULL_MULTI_VALUE_STRINGS);
    if (allowNestedArrays == ExpressionProcessing.allowNestedArrays() &&
        homogenizeNullMultiValueStrings == ExpressionProcessing.isHomogenizeNullMultiValueStringArrays()) {
      return runWithNullHandingOptions();
    }
    ExpressionProcessingConfig prevExprConfig = ExpressionProcessing.currentConfig();
    try {
      ExpressionProcessing.initializeForTests(allowNestedArrays);
      if (homogenizeNullMultiValueStrings) {
        ExpressionProcessing.initializeForHomogenizeNullMultiValueStrings();
      }
      return runWithNullHandingOptions();
    }
    finally {
      ExpressionProcessing.restoreConfig(prevExprConfig);
    }
  }

  private ActualResults runWithNullHandingOptions()
  {
    // Horrible, hacky way to change the way Druid handles
    // nulls. The config is meant to be global, initialized
    // on startup. This is a cheap workaround.
    // Only works for single-threaded tests.
    String sqlNullHandling = testCase.option(OptionsSection.SQL_COMPATIBLE_NULLS);
    if (sqlNullHandling == null) {
      return runWithCustomPlanner();
    }
    boolean useSqlNulls = QueryContexts.getAsBoolean(
            OptionsSection.SQL_COMPATIBLE_NULLS,
            sqlNullHandling,
            true);
    if (useSqlNulls != NullHandling.sqlCompatible()) {
      return null;
    }
    return runWithCustomPlanner();
  }

  /**
   * The planner factory and surrounding objects are designed to be created once
   * at the start of a Druid run. Test cases, however, want to try variations.
   * If the test case has planner settings, create a new planner fixture
   * (and all its associated knick-knacks), just for that one test. The custom
   * planner starts with the configuration for the "global" planner.
   * <p>
   * To do: since we want to test the planner, restructure the code to allow
   * changing just the planner config without needing to rebuild everything
   * else.
   * <p>
   * The planner fixture (and its associated mock segments) also must be
   * recreated if the global options change, such as null handling. Again, ugly,
   * but the best we can do.
   * <p>
   * The planner fixture is not global, so we can create a new one just for
   * this test, leaving the original one unchanged.
   */
  private ActualResults runWithCustomPlanner()
  {
    PlannerConfig plannerConfig = baseFixture.plannerConfig();
    if (testCase.requiresCustomPlanner()) {
      plannerConfig = QueryTestCases.applyOptions(
          plannerConfig,
          testCase.optionsSection().options());
    }
    SqlStatementFactory stmtFactory = baseFixture.statementFactory(plannerConfig);
    ActualResults results = new ActualResults(testCase);
    new QueryTestCasePlanner(
        baseFixture,
        stmtFactory,
        testCase,
        results
    ).run();
    return results;
  }
}
