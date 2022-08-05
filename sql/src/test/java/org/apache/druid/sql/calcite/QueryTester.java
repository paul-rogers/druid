package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.DirectStatement;
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
import org.junit.Assert;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

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

    public QueryTestCase testCase(SqlQueryPlus queryPlus)
    {
      return new QueryTestCase(this, queryPlus);
    }
  }

  public static class QueryTestCase
  {
    private final QueryTestConfig testConfig;
    private final SqlQueryPlus queryPlus;
    private PlannerConfig plannerConfig;
    private AuthorizerMapper authorizerMapper;
    private List<Query<?>> expectedQueries;
    private ResultsVerifier expectedResultsVerifier;

    public QueryTestCase(QueryTestConfig testConfig, SqlQueryPlus queryPlus)
    {
      this.testConfig = testConfig;
      this.queryPlus = queryPlus;
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

    public QueryTester tester()
    {
      return new QueryTester(this);
    }
  }

  final QueryTestConfig testConfig;
  final QueryTestCase testCase;

  public QueryTester(QueryTestCase testCase)
  {
    this.testConfig = testCase.testConfig;
    this.testCase = testCase;
  }

  public Pair<RowSignature, List<Object[]>> run()
  {
    final SqlStatementFactory sqlLifecycleFactory = getSqlLifecycleFactory();
    final DirectStatement stmt = sqlLifecycleFactory.directStatement(testCase.queryPlus);
    Sequence<Object[]> results = stmt.execute();
    RelDataType rowType = stmt.prepareResult().getRowType();
    return new Pair<>(
        RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType),
        results.toList()
    );
  }

  public SqlStatementFactory getSqlLifecycleFactory()
  {
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

  public void verifyResults(final Pair<RowSignature, List<Object[]>> results)
  {
    for (int i = 0; i < results.rhs.size(); i++) {
      log.info("row #%d: %s", i, Arrays.toString(results.rhs.get(i)));
    }

    testCase.expectedResultsVerifier.verifyRowSignature(results.lhs);
    testCase.expectedResultsVerifier.verify(testCase.queryPlus.sql(), results.rhs);
  }

  public void verifyQueries(final List<Query<?>> recordedQueries)
  {
    if (testCase.expectedQueries == null) {
      return;
    }

    Assert.assertEquals(
        StringUtils.format("query count: %s", testCase.queryPlus.sql()),
        testCase.expectedQueries.size(),
        recordedQueries.size()
    );
    for (int i = 0; i < testCase.expectedQueries.size(); i++) {
      Assert.assertEquals(
          StringUtils.format("query #%d: %s", i + 1, testCase.queryPlus.sql()),
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
}
