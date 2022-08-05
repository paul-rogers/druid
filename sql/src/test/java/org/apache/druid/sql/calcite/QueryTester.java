package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.view.InProcessViewManager;

import java.util.List;

public class QueryTester
{
  public static class QueryTestConfig
  {
    final QueryRunnerFactoryConglomerate conglomerate;
    final SpecificSegmentsQuerySegmentWalker walker;
    final ObjectMapper objectMapper;
    final DruidOperatorTable operatorTable;
    final ExprMacroTable macroTable;
    final AuthConfig authConfig;

    public QueryTestConfig(
        final QueryRunnerFactoryConglomerate conglomerate,
        final SpecificSegmentsQuerySegmentWalker walker,
        final ObjectMapper objectMapper,
        final DruidOperatorTable operatorTable,
        final ExprMacroTable macroTable,
        final AuthConfig authConfig
    )
    {
      this.conglomerate = conglomerate;
      this.walker = walker;
      this.objectMapper = objectMapper;
      this.operatorTable = operatorTable;
      this.macroTable = macroTable;
      this.authConfig = authConfig;
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
}
