package org.apache.druid.sql.calcite.tester;

import com.google.inject.Injector;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.tester.PlannerTestFixture.Builder;
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
