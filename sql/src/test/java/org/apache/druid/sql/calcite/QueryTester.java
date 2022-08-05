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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.PreparedStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest.ResultsVerifier;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.apache.druid.sql.http.SqlParameter;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class QueryTester
{
  public static final Logger log = new Logger(QueryTester.class);

  public static class QueryTestConfig
  {
    final QueryRunnerFactoryConglomerate conglomerate;
    final SpecificSegmentsQuerySegmentWalker walker;
    final ObjectMapper objectMapper;
    final DruidOperatorTable operatorTable;
    final ExprMacroTable macroTable;
    final AuthConfig authConfig;
    final QueryLogHook queryLogHook;

    public QueryTestConfig(
        final QueryRunnerFactoryConglomerate conglomerate,
        final SpecificSegmentsQuerySegmentWalker walker,
        final ObjectMapper objectMapper,
        final DruidOperatorTable operatorTable,
        final ExprMacroTable macroTable,
        final AuthConfig authConfig,
        final QueryLogHook queryLogHook
    )
    {
      this.conglomerate = conglomerate;
      this.walker = walker;
      this.objectMapper = objectMapper;
      this.operatorTable = operatorTable;
      this.macroTable = macroTable;
      this.authConfig = authConfig;
      this.queryLogHook = queryLogHook;
    }

    public QueryTestCase testCase()
    {
      return new QueryTestCase(this);
    }
  }

  /**
   * Fluent SQL test case builder. Gathers inputs and expected outputs.
   * Can be run in a variety of ways: verify queries & results. verify
   * analysis, run to produce results, etc.
   * <p>
   * For convenience, all fields are optional, which allows a test
   * case to be built up in layers of function calls. The absolute bare
   * minimum is to provide a SQL statement and authorization result:
   * that is all that is needed to run a query to produce results, if the
   * default is adequate for other values.
   * <p>
   * Fields are checked when required. For example the SQL statement
   * is always required to do anything with a test; the expected results
   * are required only if the caller chooses to verify results.
   * <p>
   * The test has a reference to a test configuration that provides
   * the constant setup: things that don't vary per test. The planner
   * configuration is special: in production, there is a single instance
   * registered in Guice. But, for tests, the planner config may vary
   * per test. if the planner config is not given for a test, then the
   * global default is used.
   * <p>
   * This class simply gathers the test attributes. To run the test, call
   * {@link #tester()} to get the object which actually runs the test.
   * A single test case can produce multiple testers, though that is
   * seldom useful. For convenience, the most common tester methods
   * appear here as well: they create the tester implicitly.
   */
  public static class QueryTestCase
  {
    private final QueryTestConfig testConfig;
    private String sql;
    private Map<String, Object> queryContext;
    private List<SqlParameter> parameters;
    private AuthenticationResult authenticationResult;
    private PlannerConfig plannerConfig;
    private AuthorizerMapper authorizerMapper;
    private List<Query<?>> expectedQueries;
    private ResultsVerifier expectedResultsVerifier;
    private Consumer<ExpectedException> expectedExceptionInitializer;
    private List<ResourceAction> expectedResources;
    private boolean skipVectorize;
    private boolean cannotVectorize;

    public QueryTestCase(QueryTestConfig testConfig)
    {
      this.testConfig = testConfig;
    }

    public QueryTestCase(QueryTestCase from)
    {
      this.testConfig = from.testConfig;
      this.sql = from.sql;
      this.queryContext = from.queryContext;
      this.parameters = from.parameters;
      this.authenticationResult = from.authenticationResult;
      this.plannerConfig = from.plannerConfig;
      this.authorizerMapper = from.authorizerMapper;
      this.expectedQueries = from.expectedQueries;
      this.expectedResultsVerifier = from.expectedResultsVerifier;
      this.expectedExceptionInitializer = from.expectedExceptionInitializer;
      this.expectedResources = from.expectedResources;
      this.skipVectorize = from.skipVectorize;
      this.cannotVectorize = from.cannotVectorize;
    }

    public String sql()
    {
      return sql;
    }

    public SqlQueryPlus queryPlus()
    {
      if (sql == null) {
        throw new ISE("Test must have SQL statement");
      }

      if (authenticationResult == null) {
        throw new ISE("Test must have an authentication result");
      }

      return SqlQueryPlus.builder(sql)
          .context(queryContext)
          .sqlParameters(parameters)
          .auth(authenticationResult)
          .build();
    }

    public Map<String, Object> context()
    {
      return queryContext == null ? ImmutableMap.of() : queryContext;
    }

    public QueryTestCase copy()
    {
      return new QueryTestCase(this);
    }

    public QueryTestCase copyWithContext(final Map<String, Object> context)
    {
      return new QueryTestCase(this)
          .context(context);
    }

    public QueryTestCase sql(String sql)
    {
      this.sql = sql;
      return this;
    }

    public QueryTestCase context(Map<String, Object> queryContext)
    {
      this.queryContext = queryContext;
      return this;
    }

    public QueryTestCase parameters(List<SqlParameter> parameters)
    {
      this.parameters = parameters;
      return this;
    }

    public QueryTestCase auth(AuthenticationResult authenticationResult)
    {
      this.authenticationResult = authenticationResult;
      return this;
    }

    public QueryTestCase plannerConfig(PlannerConfig plannerConfig)
    {
      this.plannerConfig = plannerConfig;
      return this;
    }

    public QueryTestCase authorizerMapper(AuthorizerMapper authorizerMapper)
    {
      this.authorizerMapper = authorizerMapper;
      return this;
    }

    public QueryTestCase expectedQueries(List<Query<?>> expectedQueries)
    {
      this.expectedQueries = expectedQueries;
      return this;
    }

    /**
     * Sets the expected native query while replacing the query context in
     * that queryey with the one held by the test case. For this to work,
     * set the context first, else an empty context is used.
     */
    public QueryTestCase expectedQueryWithContext(Query<?> expectedQuery)
    {
      this.expectedQueries =
          expectedQuery == null
          ? Collections.emptyList()
          : Collections.singletonList(
              QueryTester.recursivelyOverrideContext(
                  expectedQuery,
                  context()));
      return this;
    }

    public QueryTestCase expectedResults(ResultsVerifier expectedResultsVerifier)
    {
      this.expectedResultsVerifier = expectedResultsVerifier;
      return this;
    }

    public QueryTestCase expectedResults(List<Object[]> expectedResults)
    {
      this.expectedResultsVerifier = new DefaultResultsVerifier(expectedResults, null);
      return this;
    }

    public QueryTestCase expectedResults(
        List<Object[]> expectedResults,
        final RowSignature expectedResultSignature)
    {
      this.expectedResultsVerifier = new DefaultResultsVerifier(
          expectedResults,
          expectedResultSignature);
      return this;
    }

    public QueryTestCase expectedExceptionInitializer(final Consumer<ExpectedException> expectedExceptionInitializer)
    {
      this.expectedExceptionInitializer = expectedExceptionInitializer;
      return this;
    }

    public QueryTestCase expectedResources(List<ResourceAction> expectedResources)
    {
      this.expectedResources = expectedResources;
      return this;
    }

    public QueryTestCase skipVectorize(boolean skip)
    {
      this.skipVectorize = skip;
      return this;
    }

    public QueryTestCase cannotVectorize(boolean option)
    {
      this.cannotVectorize = option;
      return this;
    }

    /**
     * Produce a {@link QueryTester} to run the test.
     */
    public QueryTester tester()
    {
      return new QueryTester(this);
    }

    /**
     * Convenience method to create the tester, run the query, and
     * verify results.
     */
    public void runAndVerify()
    {
      tester().runAndVerify();
    }

    /**
     * Runs and verifies the query once or twice, depending on
     * settings:
     * <ul>
     * <li>{@code vectorize=false} - in all cases.</li>
     * <li>{@code vectorize=force} - if the query is vectorizable.
     * That is, if {@code skipVectorize} is not {@code false}. (Sorry for the
     * double negative.)</li>
     * </ul>
     * <p>
     * A new test case is created with the proper context value set. Rewrites
     * the expected native queries to include that context.
     *
     * @param expectedException the expected exception, if the query should
     * fail, else {@code null}.
     */
    public void runAndVerifyVectorized(ExpectedException expectedException)
    {
      final List<String> vectorizeValues = new ArrayList<>();

      vectorizeValues.add("false");

      if (!skipVectorize) {
        vectorizeValues.add("force");
      }

      for (final String vectorize : vectorizeValues) {
        final Map<String, Object> theQueryContext = new HashMap<>(queryContext);
        theQueryContext.put(QueryContexts.VECTORIZE_KEY, vectorize);
        theQueryContext.put(QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize);

        if (!"false".equals(vectorize)) {
          theQueryContext.put(QueryContexts.VECTOR_SIZE_KEY, 2); // Small vector size to ensure we use more than one.
        }

        final List<Query<?>> theQueries = new ArrayList<>();
        for (Query<?> query : expectedQueries) {
          theQueries.add(recursivelyOverrideContext(query, theQueryContext));
        }

        if (cannotVectorize && "force".equals(vectorize)) {
          expectedException.expect(RuntimeException.class);
          expectedException.expectMessage("Cannot vectorize");
        } else if (expectedExceptionInitializer != null) {
          expectedExceptionInitializer.accept(expectedException);
        }

        copyWithContext(theQueryContext)
            .expectedQueries(theQueries)
            .runAndVerify();
      }
    }
  }

  final QueryTestConfig testConfig;
  final QueryTestCase testCase;

  public QueryTester(QueryTestCase testCase)
  {
    this.testConfig = testCase.testConfig;
    this.testCase = testCase;
  }

  public void runAndVerify()
  {
    testConfig.queryLogHook.clearRecordedQueries();
    final DirectStatement stmt = directStmt();
    Sequence<Object[]> results = stmt.execute();
    verifyResults(results.toList());
    testCase.expectedResultsVerifier.verifyRowSignature(rowSignature(stmt));
    verifyQueries(testConfig.queryLogHook.getRecordedQueries());
    if (testCase.expectedResources != null) {
      Assert.assertEquals(
          ImmutableSet.copyOf(testCase.expectedResources),
          stmt.resources()
      );
    }
  }

  private DirectStatement directStmt()
  {
    final SqlStatementFactory sqlLifecycleFactory = getSqlLifecycleFactory();
    return sqlLifecycleFactory.directStatement(testCase.queryPlus());
  }

  private RowSignature rowSignature(DirectStatement stmt)
  {
    RelDataType rowType = stmt.prepareResult().getRowType();
    return RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType);
  }

  public Pair<RowSignature, List<Object[]>> run()
  {
    final DirectStatement stmt = directStmt();
    Sequence<Object[]> results = stmt.execute();
    return new Pair<>(
        rowSignature(stmt),
        results.toList()
    );
  }

  public SqlStatementFactory getSqlLifecycleFactory()
  {
    if (testCase.plannerConfig == null) {
      throw new ISE("Test must have a planner config");
    }

    if (testCase.authorizerMapper == null) {
      throw new ISE("Test must have an authorizer mapper");
    }

    final InProcessViewManager viewManager = new InProcessViewManager(CalciteTests.DRUID_VIEW_MACRO_FACTORY);
    DruidSchemaCatalog rootSchema = CalciteTests.createMockRootSchema(
        testConfig.conglomerate,
        testConfig.walker,
        testCase.plannerConfig,
        viewManager,
        new NoopDruidSchemaManager(),
        testCase.authorizerMapper
    );

    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        new TestQueryMakerFactory(
            CalciteTests.createMockQueryLifecycleFactory(testConfig.walker, testConfig.conglomerate),
            testConfig.objectMapper
        ),
        testConfig.operatorTable,
        testConfig.macroTable,
        testCase.plannerConfig,
        testCase.authorizerMapper,
        testConfig.objectMapper,
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of())
    );
    final SqlStatementFactory sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(plannerFactory, testConfig.authConfig);

    viewManager.createView(
        plannerFactory,
        "aview",
        "SELECT SUBSTRING(dim1, 1, 1) AS dim1_firstchar FROM foo WHERE dim2 = 'a'"
    );

    viewManager.createView(
        plannerFactory,
        "bview",
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'"
    );

    viewManager.createView(
        plannerFactory,
        "cview",
        "SELECT SUBSTRING(bar.dim1, 1, 1) AS dim1_firstchar, bar.dim2 as dim2, dnf.l2 as l2\n"
        + "FROM (SELECT * from foo WHERE dim2 = 'a') as bar INNER JOIN druid.numfoo dnf ON bar.dim2 = dnf.dim2"
    );

    viewManager.createView(
        plannerFactory,
        "dview",
        "SELECT SUBSTRING(dim1, 1, 1) AS numfoo FROM foo WHERE dim2 = 'a'"
    );

    viewManager.createView(
        plannerFactory,
        "forbiddenView",
        "SELECT __time, SUBSTRING(dim1, 1, 1) AS dim1_firstchar, dim2 FROM foo WHERE dim2 = 'a'"
    );

    viewManager.createView(
        plannerFactory,
        "restrictedView",
        "SELECT __time, dim1, dim2, m1 FROM druid.forbiddenDatasource WHERE dim2 = 'a'"
    );

    viewManager.createView(
        plannerFactory,
        "invalidView",
        "SELECT __time, dim1, dim2, m1 FROM druid.invalidDatasource WHERE dim2 = 'a'"
    );
    return sqlLifecycleFactory;
  }

  public void verifyResults(List<Object[]> results)
  {
    for (int i = 0; i < results.size(); i++) {
      log.info("row #%d: %s", i, Arrays.toString(results.get(i)));
    }

    testCase.expectedResultsVerifier.verify(testCase.sql(), results);
  }

  public void verifyQueries(final List<Query<?>> recordedQueries)
  {
    if (testCase.expectedQueries == null) {
      return;
    }

    Assert.assertEquals(
        StringUtils.format("query count: %s", testCase.sql()),
        testCase.expectedQueries.size(),
        recordedQueries.size()
    );
    for (int i = 0; i < testCase.expectedQueries.size(); i++) {
      Assert.assertEquals(
          StringUtils.format("query #%d: %s", i + 1, testCase.sql()),
          testCase.expectedQueries.get(i),
          recordedQueries.get(i)
      );

      try {
        // go through some JSON serde and back, round tripping both queries and comparing them to each other, because
        // Assert.assertEquals(recordedQueries.get(i), stringAndBack) is a failure due to a sorted map being present
        // in the recorded queries, but it is a regular map after deserialization
        final String recordedString = testConfig.objectMapper.writeValueAsString(recordedQueries.get(i));
        final Query<?> stringAndBack = testConfig.objectMapper.readValue(recordedString, Query.class);
        final String expectedString = testConfig.objectMapper.writeValueAsString(testCase.expectedQueries.get(i));
        final Query<?> expectedStringAndBack = testConfig.objectMapper.readValue(expectedString, Query.class);
        Assert.assertEquals(expectedStringAndBack, stringAndBack);
      }
      catch (JsonProcessingException e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  public static class DefaultResultsVerifier implements ResultsVerifier
  {
    protected final List<Object[]> expectedResults;
    @Nullable
    protected final RowSignature expectedResultRowSignature;

    public DefaultResultsVerifier(List<Object[]> expectedResults, RowSignature expectedSignature)
    {
      this.expectedResults = expectedResults;
      this.expectedResultRowSignature = expectedSignature;
    }

    @Override
    public void verifyRowSignature(RowSignature rowSignature)
    {
      if (expectedResultRowSignature != null) {
        Assert.assertEquals(expectedResultRowSignature, rowSignature);
      }
    }

    @Override
    public void verify(String sql, List<Object[]> results)
    {
      Assert.assertEquals(StringUtils.format("result count: %s", sql), expectedResults.size(), results.size());
      assertResultsEquals(sql, expectedResults, results);
    }
  }

  public static void assertResultsEquals(String sql, List<Object[]> expectedResults, List<Object[]> results)
  {
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Object[] expectedResult = expectedResults.get(i);
      Object[] result = results.get(i);
      Assert.assertEquals(expectedResult.length, result.length);
      for (int j = 0; j < expectedResult.length; j++) {
        String msg = StringUtils.format("result #%d[%d]: %s", i + 1, j, sql);
        if (expectedResult[j] instanceof Float) {
          Assert.assertEquals(msg, (Float) expectedResult[j], (Float) result[j], 0.000001);
        } else if (expectedResult[j] instanceof Double) {
          Assert.assertEquals(msg, (Double) expectedResult[j], (Double) result[j], 0.000001);
        } else {
          Assert.assertEquals(msg, expectedResult[j], result[j]);
        }
      }
    }
  }

  /**
   * Override not just the outer query context, but also the contexts of all subqueries.
   */
  public static <T> Query<T> recursivelyOverrideContext(final Query<T> query, final Map<String, Object> context)
  {
    return query.withDataSource(recursivelyOverrideContext(query.getDataSource(), context))
                .withOverriddenContext(context);
  }

  /**
   * Override the contexts of all subqueries of a particular datasource.
   */
  private static DataSource recursivelyOverrideContext(final DataSource dataSource, final Map<String, Object> context)
  {
    if (dataSource instanceof QueryDataSource) {
      final Query<?> subquery = ((QueryDataSource) dataSource).getQuery();
      return new QueryDataSource(recursivelyOverrideContext(subquery, context));
    } else {
      return dataSource.withChildren(
          dataSource.getChildren()
                    .stream()
                    .map(ds -> recursivelyOverrideContext(ds, context))
                    .collect(Collectors.toList())
      );
    }
  }

  public PreparedStatement preparedStmt()
  {
    SqlStatementFactory lifecycleFactory = getSqlLifecycleFactory();
    return lifecycleFactory.preparedStatement(testCase.queryPlus());
  }

  public Set<ResourceAction> analyzeResources()
  {
    PreparedStatement stmt = preparedStmt();
    stmt.prepare();
    return stmt.allResources();
  }

  public void verifyValidationError(Matcher<Throwable> validationErrorMatcher)
  {
    testConfig.queryLogHook.clearRecordedQueries();
    DirectStatement stmt = directStmt();

    final Throwable e = Assert.assertThrows(
        Throwable.class,
        () -> {
          stmt.execute();
        }
    );

    MatcherAssert.assertThat(e, validationErrorMatcher);
    Assert.assertTrue(testConfig.queryLogHook.getRecordedQueries().isEmpty());
  }
}
