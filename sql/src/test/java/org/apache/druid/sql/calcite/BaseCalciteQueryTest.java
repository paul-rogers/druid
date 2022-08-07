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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.annotations.UsedByJUnitParamsRunner;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.VersionOneHyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.QueryTester.QueryTestCase;
import org.apache.druid.sql.calcite.QueryTester.QueryTestConfig;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A base class for SQL query testing. It sets up query execution environment, provides useful helper methods,
 * and populates data using {@link CalciteTests#createMockWalker}.
 */
public class BaseCalciteQueryTest extends CalciteTestBase
{
  public static String NULL_STRING;
  public static Float NULL_FLOAT;
  public static Long NULL_LONG;
  public static final String HLLC_STRING = VersionOneHyperLogLogCollector.class.getName();

  @BeforeClass
  public static void setup()
  {
    setupNullValues();
  }

  public static void setupNullValues()
  {
    NULL_STRING = NullHandling.defaultStringValue();
    NULL_FLOAT = NullHandling.defaultFloatValue();
    NULL_LONG = NullHandling.defaultLongValue();
  }

  public static final Logger log = new Logger(BaseCalciteQueryTest.class);

  public static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();
  public static final PlannerConfig PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE =
      PlannerConfig.builder().serializeComplexValues(false).build();

  public static final PlannerConfig PLANNER_CONFIG_REQUIRE_TIME_CONDITION =
      PlannerConfig.builder().requireTimeCondition(true).build();

  public static final PlannerConfig PLANNER_CONFIG_NO_TOPN =
      PlannerConfig.builder().maxTopNLimit(0).build();

  public static final PlannerConfig PLANNER_CONFIG_NO_HLL =
      PlannerConfig.builder().useApproximateCountDistinct(false).build();

  public static final String LOS_ANGELES = "America/Los_Angeles";
  public static final PlannerConfig PLANNER_CONFIG_LOS_ANGELES =
      PlannerConfig
          .builder()
          .sqlTimeZone(DateTimes.inferTzFromString(LOS_ANGELES))
          .build();

  public static final PlannerConfig PLANNER_CONFIG_AUTHORIZE_SYS_TABLES =
      PlannerConfig.builder().authorizeSystemTablesDirectly(true).build();

  public static final PlannerConfig PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN =
      PlannerConfig.builder().useNativeQueryExplain(true).build();

  public static final int MAX_NUM_IN_FILTERS = 100;
  public static final PlannerConfig PLANNER_CONFIG_MAX_NUMERIC_IN_FILTER =
      PlannerConfig.builder().maxNumericInFilters(MAX_NUM_IN_FILTERS).build();

  public static final String DUMMY_SQL_ID = "dummy";

  private static final ImmutableMap.Builder<String, Object> DEFAULT_QUERY_CONTEXT_BUILDER =
      ImmutableMap.<String, Object>builder()
                  .put(PlannerContext.CTX_SQL_QUERY_ID, DUMMY_SQL_ID)
                  .put(PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z")
                  .put(QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS)
                  .put(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE);
  public static final Map<String, Object> QUERY_CONTEXT_DEFAULT = DEFAULT_QUERY_CONTEXT_BUILDER.build();

  public static final Map<String, Object> QUERY_CONTEXT_NO_STRINGIFY_ARRAY =
      DEFAULT_QUERY_CONTEXT_BUILDER.put(PlannerContext.CTX_SQL_STRINGIFY_ARRAYS, false)
                                   .build();

  public static final Map<String, Object> QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS = ImmutableMap.of(
      PlannerContext.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      TimeseriesQuery.SKIP_EMPTY_BUCKETS, false,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  public static final Map<String, Object> QUERY_CONTEXT_DO_SKIP_EMPTY_BUCKETS = ImmutableMap.of(
      PlannerContext.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      TimeseriesQuery.SKIP_EMPTY_BUCKETS, true,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  public static final Map<String, Object> QUERY_CONTEXT_NO_TOPN = ImmutableMap.of(
      PlannerContext.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, "false",
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  public static final Map<String, Object> QUERY_CONTEXT_LOS_ANGELES = ImmutableMap.of(
      PlannerContext.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      PlannerContext.CTX_SQL_TIME_ZONE, LOS_ANGELES,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  // Matches QUERY_CONTEXT_DEFAULT
  public static final Map<String, Object> TIMESERIES_CONTEXT_BY_GRAN = ImmutableMap.of(
      PlannerContext.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z",
      TimeseriesQuery.SKIP_EMPTY_BUCKETS, true,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  // Add additional context to the given context map for when the
  // timeseries query has timestamp_floor expression on the timestamp dimension
  public static Map<String, Object> getTimeseriesContextWithFloorTime(
      Map<String, Object> context,
      String timestampResultField
  )
  {
    return ImmutableMap.<String, Object>builder()
                       .putAll(context)
                       .put(TimeseriesQuery.CTX_TIMESTAMP_RESULT_FIELD, timestampResultField)
                       .build();
  }

  // Matches QUERY_CONTEXT_LOS_ANGELES
  public static final Map<String, Object> TIMESERIES_CONTEXT_LOS_ANGELES = new HashMap<>();

  public static final Map<String, Object> OUTER_LIMIT_CONTEXT = new HashMap<>(QUERY_CONTEXT_DEFAULT);

  public static QueryRunnerFactoryConglomerate conglomerate;
  public static Closer resourceCloser;
  public static int minTopNThreshold = TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;

  final boolean useDefault = NullHandling.replaceWithDefault();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();


  public boolean cannotVectorize = false;
  public boolean skipVectorize = false;

  public SpecificSegmentsQuerySegmentWalker walker = null;
  public QueryLogHook queryLogHook;

  static {
    TIMESERIES_CONTEXT_LOS_ANGELES.put(PlannerContext.CTX_SQL_QUERY_ID, DUMMY_SQL_ID);
    TIMESERIES_CONTEXT_LOS_ANGELES.put(PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z");
    TIMESERIES_CONTEXT_LOS_ANGELES.put(PlannerContext.CTX_SQL_TIME_ZONE, LOS_ANGELES);
    TIMESERIES_CONTEXT_LOS_ANGELES.put(TimeseriesQuery.SKIP_EMPTY_BUCKETS, true);
    TIMESERIES_CONTEXT_LOS_ANGELES.put(QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS);
    TIMESERIES_CONTEXT_LOS_ANGELES.put(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE);

    OUTER_LIMIT_CONTEXT.put(PlannerContext.CTX_SQL_OUTER_LIMIT, 2);
  }

  @BeforeClass
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser, () -> minTopNThreshold);
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Rule
  public QueryLogHook getQueryLogHook()
  {
    return queryLogHook = QueryLogHook.create(queryJsonMapper());
  }

  @Before
  public void setUp() throws Exception
  {
    walker = createQuerySegmentWalker();
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker() throws IOException
  {
    return CalciteTests.createMockWalker(
        conglomerate,
        temporaryFolder.newFolder()
    );
  }

  public void assertQueryIsUnplannable(final String sql, String expectedError)
  {
    assertQueryIsUnplannable(PLANNER_CONFIG_DEFAULT, sql, expectedError);
  }

  public void assertQueryIsUnplannable(final PlannerConfig plannerConfig, final String sql, String expectedError)
  {
    Exception e = null;
    try {
      testQuery(plannerConfig, sql, CalciteTests.REGULAR_USER_AUTH_RESULT, ImmutableList.of(), ImmutableList.of());
    }
    catch (Exception e1) {
      e = e1;
    }

    if (!(e instanceof RelOptPlanner.CannotPlanException)) {
      log.error(e, "Expected CannotPlanException for query: %s", sql);
      Assert.fail(sql);
    }
    Assert.assertEquals(sql,
        StringUtils.format("Cannot build plan for query: %s. %s", sql, expectedError),
        e.getMessage());
  }

  /**
   * Provided for tests that wish to check multiple queries instead of relying on ExpectedException.
   */
  public void assertQueryIsForbidden(final String sql, final AuthenticationResult authenticationResult)
  {
    assertQueryIsForbidden(PLANNER_CONFIG_DEFAULT, sql, authenticationResult);
  }

  public void assertQueryIsForbidden(
      final PlannerConfig plannerConfig,
      final String sql,
      final AuthenticationResult authenticationResult
  )
  {
    Exception e = null;
    try {
      testQuery(plannerConfig, sql, authenticationResult, ImmutableList.of(), ImmutableList.of());
    }
    catch (Exception e1) {
      e = e1;
    }

    if (!(e instanceof ForbiddenException)) {
      log.error(e, "Expected ForbiddenException for query: %s with authResult: %s", sql, authenticationResult);
      Assert.fail(sql);
    }
  }

  public void testQuery(
      final String sql,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        expectedQueries,
        expectedResults
    );
  }

  public void testQuery(
      final String sql,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults,
      final RowSignature expectedResultRowSignature
  )
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        expectedQueries,
        expectedResults,
        expectedResultRowSignature
    );
  }

  public void testQuery(
      final String sql,
      final Map<String, Object> context,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        context,
        DEFAULT_PARAMETERS,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        expectedQueries,
        expectedResults
    );
  }

  public void testQuery(
      final String sql,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults,
      final List<SqlParameter> parameters
  )
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        parameters,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        expectedQueries,
        expectedResults
    );
  }

  public void testQuery(
      final PlannerConfig plannerConfig,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    testQuery(
        plannerConfig,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        sql,
        authenticationResult,
        expectedQueries,
        expectedResults
    );
  }

  public void testQuery(
      final String sql,
      final Map<String, Object> context,
      final List<Query<?>> expectedQueries,
      final ResultsVerifier expectedResultsVerifier
  )
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        context,
        DEFAULT_PARAMETERS,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        expectedQueries,
        expectedResultsVerifier,
        null
    );
  }

  /**
   * Run the query with the context provided. Does not run the
   * query with multiple vectorize options.
   */
  public void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    log.info("SQL: %s", sql);
    testCase()
        .sql(sql)
        .context(queryContext)
        .auth(authenticationResult)
        .plannerConfig(plannerConfig)
        .authorizerMapper(injector().getInstance(AuthorizerMapper.class))
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResults)
        .runAndVerify();
  }

  public void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final List<SqlParameter> parameters,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    testQueryVectorized(
        testCase()
            .sql(sql)
            .context(queryContext)
            .parameters(parameters)
            .auth(authenticationResult)
            .plannerConfig(plannerConfig)
            .expectedQueries(expectedQueries)
            .expectedResults(expectedResults)
    );
  }

  public void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final List<SqlParameter> parameters,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults,
      final RowSignature expectedResultSignature
  )
  {
    testQueryVectorized(
        testCase()
            .sql(sql)
            .context(queryContext)
            .parameters(parameters)
            .auth(authenticationResult)
            .plannerConfig(plannerConfig)
            .expectedQueries(expectedQueries)
            .expectedResults(expectedResults, expectedResultSignature)
    );
  }

  public void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final List<SqlParameter> parameters,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query<?>> expectedQueries,
      final ResultsVerifier expectedResultsVerifier,
      @Nullable final Consumer<ExpectedException> expectedExceptionInitializer
  )
  {
    testQueryVectorized(
        testCase()
            .sql(sql)
            .context(queryContext)
            .parameters(parameters)
            .auth(authenticationResult)
            .plannerConfig(plannerConfig)
            .expectedQueries(expectedQueries)
            .expectedResults(expectedResultsVerifier)
            .expectedExceptionInitializer(expectedExceptionInitializer)
    );
  }

  public void testQueryVectorized(
      final QueryTestCase testCase
  )
  {
    log.info("SQL: %s", testCase.sql());

    testCase
        .authorizerMapper(injector().getInstance(AuthorizerMapper.class))
        .cannotVectorize(cannotVectorize)
        .skipVectorize(skipVectorize)
        .runAndVerifyVectorized(expectedException);
  }

  protected QueryTestConfig testConfig()
  {
    return new QueryTestConfig(
        injector(),
        conglomerate,
        walker,
        new AuthConfig(),
        queryLogHook
    );
  }

  protected QueryTestCase testCase()
  {
    return testConfig().testCase();
  }

  public void testQueryThrows(final String sql, Consumer<ExpectedException> expectedExceptionInitializer)
  {
    testQueryThrows(
        sql,
        new HashMap<>(QUERY_CONTEXT_DEFAULT),
        ImmutableList.of(),
        expectedExceptionInitializer
    );
  }

  public void testQueryThrows(
      final String sql,
      final Map<String, Object> queryContext,
      final List<Query<?>> expectedQueries,
      final Consumer<ExpectedException> expectedExceptionInitializer
  )
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        queryContext,
        DEFAULT_PARAMETERS,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        expectedQueries,
        (query, results) -> {},
        expectedExceptionInitializer
    );
  }

  public Set<ResourceAction> analyzeResources(
      PlannerConfig plannerConfig,
      String sql,
      AuthenticationResult authenticationResult
  )
  {
    return analyzeResources(
        plannerConfig,
        new AuthConfig(),
        sql,
        ImmutableMap.of(),
        authenticationResult
    );
  }

  public Set<ResourceAction> analyzeResources(
      PlannerConfig plannerConfig,
      AuthConfig authConfig,
      String sql,
      Map<String, Object> contexts,
      AuthenticationResult authenticationResult
  )
  {
    return new QueryTestConfig(
            injector(),
            conglomerate,
            walker,
            authConfig,
            queryLogHook
         )
        .testCase()
        .sql(sql)
        .context(contexts)
        .auth(authenticationResult)
        .plannerConfig(plannerConfig)
        .authorizerMapper(injector().getInstance(AuthorizerMapper.class))
        .tester()
        .analyzeResources();
  }

  protected void cannotVectorize()
  {
    cannotVectorize = true;
  }

  protected void skipVectorize()
  {
    skipVectorize = true;
  }

  protected static boolean isRewriteJoinToFilter(final Map<String, Object> queryContext)
  {
    return (boolean) queryContext.getOrDefault(
        QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY,
        QueryContexts.DEFAULT_ENABLE_REWRITE_JOIN_TO_FILTER
    );
  }

  /**
   * This is a provider of query contexts that should be used by join tests.
   * It tests various configs that can be passed to join queries. All the configs provided by this provider should
   * have the join query engine return the same results.
   */
  public static class QueryContextForJoinProvider
  {
    @UsedByJUnitParamsRunner
    public static Object[] provideQueryContexts()
    {
      return new Object[]{
          // default behavior
          QUERY_CONTEXT_DEFAULT,
          // all rewrites enabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true)
              .build(),
          // filter-on-value-column rewrites disabled, everything else enabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, false)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true)
              .build(),
          // filter rewrites fully disabled, join-to-filter enabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, false)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true)
              .build(),
          // filter rewrites disabled, but value column filters still set to true (it should be ignored and this should
          // behave the same as the previous context)
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true)
              .build(),
          // filter rewrites fully enabled, join-to-filter disabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, false)
              .build(),
          // all rewrites disabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, false)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, false)
              .build(),
          };
    }
  }

  protected Map<String, Object> withLeftDirectAccessEnabled(Map<String, Object> context)
  {
    // since context is usually immutable in tests, make a copy
    HashMap<String, Object> newContext = new HashMap<>(context);
    newContext.put(QueryContexts.SQL_JOIN_LEFT_SCAN_DIRECT, true);
    return newContext;
  }

  /**
   * Reset the walker and conglomerate with required number of merge buffers. Default value is 2.
   */
  protected void requireMergeBuffers(int numMergeBuffers) throws IOException
  {
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
        resourceCloser,
        QueryStackTests.getProcessingConfig(true, numMergeBuffers)
    );
    walker = CalciteTests.createMockWalker(conglomerate, temporaryFolder.newFolder());
  }

  protected Map<String, Object> withTimestampResultContext(
      Map<String, Object> input,
      String timestampResultField,
      int timestampResultFieldIndex,
      Granularity granularity
  )
  {
    Map<String, Object> output = new HashMap<>(input);
    output.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD, timestampResultField);
    output.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY, granularity);
    output.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_INDEX, timestampResultFieldIndex);
    return output;
  }

  @FunctionalInterface
  public interface ResultsVerifier
  {
    default void verifyRowSignature(RowSignature rowSignature)
    {
      // do nothing
    }

    void verify(String sql, List<Object[]> results);
  }
}
