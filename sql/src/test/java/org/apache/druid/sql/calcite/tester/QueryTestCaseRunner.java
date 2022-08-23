package org.apache.druid.sql.calcite.tester;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryContexts.Vectorize;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest.QueryContextForJoinProvider;

import java.util.List;
import java.util.Map;

public class QueryTestCaseRunner
{
  public static final Logger log = new Logger(QueryTestCaseRunner.class);

  private static final Map<String, Object> ENABLE_VECTORIZE_CONTEXT =
      ImmutableMap.of(
          QueryContexts.VECTORIZE_KEY,
          Vectorize.FORCE.name(),
          QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY,
          Vectorize.FORCE.name(),
          QueryContexts.VECTOR_SIZE_KEY,
          2); // Small vector size to ensure we use more than one.
  private static final Map<String, Object> DISABLE_VECTORIZE_CONTEXT =
      ImmutableMap.of(
          QueryContexts.VECTORIZE_KEY,
          Vectorize.FALSE.name(),
          QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY,
          Vectorize.FALSE.name());

  private final PlannerTestFixture plannerFixture;
  private final SqlStatementFactory statementFactory;
  private final QueryTestCase testCase;
  private final ActualResults results;

  public QueryTestCaseRunner(
      PlannerTestFixture plannerFixture,
      SqlStatementFactory statementFactory,
      QueryTestCase testCase,
      ActualResults results
  )
  {
    this.plannerFixture = plannerFixture;
    this.testCase = testCase;
    this.results = results;
    this.statementFactory = statementFactory;
  }

  public void run()
  {
    // Run the query with the requested options
    for (QueryRun run : testCase.runs()) {
      runQuery(run);
    }
  }

  private interface QueryExec
  {
    void run(SqlQueryPlus queryPlus, Map<String, String> options);
  }

  private class ConcreteExec implements QueryExec
  {
    private QueryRun queryRun;

    private ConcreteExec(QueryRun queryRun)
    {
      this.queryRun = queryRun;
    }

    @Override
    public void run(SqlQueryPlus queryPlus, Map<String, String> options)
    {
      try (DirectStatement stmt = statementFactory.directStatement(queryPlus)) {
        List<Object[]> rows = stmt.execute().toList();
        results.run(queryRun, queryPlus.context().getUserParams(), rows, plannerFixture.jsonMapper());
      }
      catch (Exception e) {
        results.runFailed(queryRun, queryPlus.context().getUserParams(), e);
      }
    }
  }

  private static class VectorizeExec implements QueryExec
  {
    private final QueryExec child;

    public VectorizeExec(QueryExec child)
    {
      this.child = child;
    }

    @Override
    public void run(SqlQueryPlus queryPlus, Map<String, String> options)
    {
      child.run(queryPlus.withOverrides(DISABLE_VECTORIZE_CONTEXT), options);
      boolean canVectorize = QueryTestCases.booleanOption(
          options,
          OptionsSection.VECTORIZE_OPTION,
          true);
      if (!canVectorize) {
        return;
      }
      child.run(queryPlus.withOverrides(ENABLE_VECTORIZE_CONTEXT), options);
    }
  }

  /**
   * Filter to only pass along runs that match the current "replace with
   * null" setting initialized externally. It matches if no options is given
   * for the run, the option is "both", or the option Boolean value matches
   * the current setting.
   */
  private static class NullStrategyFilter implements QueryExec
  {
    private final boolean sqlCompatible = NullHandling.sqlCompatible();
    private final QueryExec child;

    public NullStrategyFilter(QueryExec child)
    {
      this.child = child;
    }

    @Override
    public void run(SqlQueryPlus queryPlus, Map<String, String> options)
    {
      String sqlNullOption = options.get(OptionsSection.SQL_COMPATIBLE_NULLS);
      if (sqlNullOption == null ||
          OptionsSection.NULL_HANDLING_BOTH.equals(sqlNullOption) ||
          QueryContexts.getAsBoolean(
              OptionsSection.SQL_COMPATIBLE_NULLS,
              sqlNullOption,
              false) == sqlCompatible) {
        child.run(queryPlus, options);
      }
    }
  }

  /**
   * Iterates over the contexts provided by QueryContextForJoinProvider,
   * which is a provider class used in JUnit, but adapted for use here.
   * The class provides not just the join options, but also a set of
   * "default" options which are the same as the defaults used in the
   * JUnit tests, so no harm in applying them.
   */
  private static class JoinContextProvider implements QueryExec
  {
    private final QueryExec child;

    public JoinContextProvider(QueryExec child)
    {
      this.child = child;
    }

    @Override
    public void run(SqlQueryPlus queryPlus, Map<String, String> options)
    {
      for (Object obj : QueryContextForJoinProvider.provideQueryContexts()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) obj;
        SqlQueryPlus rewritten = queryPlus.withOverrides(context);
        child.run(rewritten, options);
      }
    }
  }

  /**
   * Special version of {@link JoinContextProvider} that filters on
   * {@code enableJoinFilterRewrite} to handle bugs in
   * {@code testLeftJoinSubqueryWithNullKeyFilter}.
   */
  private static class JoinContextProviderFilterRewriteFilter implements QueryExec
  {
    private final QueryExec child;
    private final boolean value;

    public JoinContextProviderFilterRewriteFilter(QueryExec child, boolean value)
    {
      this.child = child;
      this.value = value;
    }

    @Override
    public void run(SqlQueryPlus queryPlus, Map<String, String> options)
    {
      for (Object obj : QueryContextForJoinProvider.provideQueryContexts()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) obj;
        // Per testLeftJoinSubqueryWithNullKeyFilter(), the default value is true.
        if (QueryTestCases.booleanOption(context, QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true) == value) {
          SqlQueryPlus rewritten = queryPlus.withOverrides(context);
          child.run(rewritten, options);
        }
      }
    }
  }

  /**
   * Special version of {@link JoinContextProvider} that filters on
   * {@code enableJoinFilterRewrite} to handle bugs in
   * {@code testLeftJoinSubqueryWithNullKeyFilter}.
   */
  private static class JoinContextProviderJoinToFilterRewriteFilter implements QueryExec
  {
    private final QueryExec child;
    private final boolean value;

    public JoinContextProviderJoinToFilterRewriteFilter(QueryExec child, boolean value)
    {
      this.child = child;
      this.value = value;
    }

    @Override
    public void run(SqlQueryPlus queryPlus, Map<String, String> options)
    {
      for (Object obj : QueryContextForJoinProvider.provideQueryContexts()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) obj;
        // Per testLeftJoinSubqueryWithNullKeyFilter(), the default value is true.
        if (BaseCalciteQueryTest.isRewriteJoinToFilter(context) == value) {
          SqlQueryPlus rewritten = queryPlus.withOverrides(context);
          child.run(rewritten, options);
        }
      }
    }
  }

  private void runQuery(QueryRun run)
  {
    SqlQueryPlus queryPlus = SqlQueryPlus
        .builder(run.testCase().sql())
        // Run with the same defaults as used in the original JUnit-based
        // tests to ensure results are consistent.
        .context(plannerFixture.applyDefaultContext(run.context()))
        .sqlParameters(run.testCase().parameters())
        .auth(plannerFixture.authResultFor(run.testCase().user()))
        .build();
    QueryExec exec = new VectorizeExec(
        new ConcreteExec(run));
    // Hard-coded support for the known providers.
    String provider = run.option(OptionsSection.PROVIDER_CLASS);
    if (provider != null) {
      switch (provider) {
        case "QueryContextForJoinProvider":
          exec = new JoinContextProvider(exec);
          break;
        case "QueryContextForJoinProviderNoFilterRewrite":
          exec = new JoinContextProviderFilterRewriteFilter(exec, false);
          break;
        case "QueryContextForJoinProviderWithFilterRewrite":
          exec = new JoinContextProviderFilterRewriteFilter(exec, true);
          break;
        case "QueryContextForJoinProviderNoRewriteJoinToFilter":
          exec = new JoinContextProviderJoinToFilterRewriteFilter(exec, false);
          break;
        case "QueryContextForJoinProviderWithRewriteJoinToFilter":
          exec = new JoinContextProviderJoinToFilterRewriteFilter(exec, true);
          break;
        default:
          log.warn("Undefined provider: %s", provider);
      }
    }
    exec = new NullStrategyFilter(exec);
    exec.run(queryPlus, run.options());
  }
}
