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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.view.DruidViewMacroFactory;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryFramework
{
  public interface QueryComponentSupplier
  {
    SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate
    ) throws IOException;

    SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper objectMapper
    );

    DruidOperatorTable createOperatorTable();

    ExprMacroTable createMacroTable();

    Iterable<? extends Module> getJacksonModules();

    Map<String, Object> getJacksonInjectables();
  }

  public static class StandardComponentSupplier implements QueryComponentSupplier
  {
    private final Injector injector;
    private final File temporaryFolder;

    public StandardComponentSupplier(
        final Injector injector,
        final File temporaryFolder)
    {
      this.injector = injector;
      this.temporaryFolder = temporaryFolder;
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate
    )
    {
      return TestDataBuilder.createMockWalker(
          injector,
          conglomerate,
          temporaryFolder
      );
    }

    @Override
    public SqlEngine createEngine(QueryLifecycleFactory qlf, ObjectMapper objectMapper)
    {
      return new NativeSqlEngine(
          qlf,
          objectMapper
      );
    }

    @Override
    public DruidOperatorTable createOperatorTable()
    {
      return QueryFrameworkUtils.createOperatorTable(injector);
    }

    @Override
    public ExprMacroTable createMacroTable()
    {
      return QueryFrameworkUtils.createExprMacroTable(injector);
    }

    @Override
    public Iterable<? extends Module> getJacksonModules()
    {
      final List<Module> modules = new ArrayList<>(new LookupSerdeModule().getJacksonModules());
      modules.add(new SimpleModule().registerSubtypes(ExternalDataSource.class));
      return modules;
    }

    @Override
    public Map<String, Object> getJacksonInjectables()
    {
      return new HashMap<>();
    }
  }

  public static class Builder
  {
    private final QueryComponentSupplier componentSupplier;
    private Injector injector;
    private int minTopNThreshold = TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;
    private int mergeBufferCount;

    public Builder(QueryComponentSupplier componentSupplier)
    {
      this.componentSupplier = componentSupplier;
    }

    public Builder injector(Injector injector)
    {
      this.injector = injector;
      return this;
    }

    public Builder minTopNThreshold(int minTopNThreshold)
    {
      this.minTopNThreshold = minTopNThreshold;
      return this;
    }

    public Builder mergeBufferCount(int mergeBufferCount)
    {
      this.mergeBufferCount = mergeBufferCount;
      return this;
    }

    public QueryFramework build()
    {
      return new QueryFramework(this);
    }
  }

  public static final DruidViewMacroFactory DRUID_VIEW_MACRO_FACTORY = new TestDruidViewMacroFactory();

  private final Closer resourceCloser = Closer.create();
  private final Injector injector;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final SpecificSegmentsQuerySegmentWalker walker;
  private final DruidOperatorTable operatorTable;
  private final ExprMacroTable macroTable;
  private final AuthorizerMapper authorizerMapper = CalciteTests.TEST_AUTHORIZER_MAPPER;
  private final ObjectMapper objectMapper;
  private final SqlEngine engine;
  private final QueryLifecycleFactory qlf;

  private QueryFramework(Builder builder)
  {
    this.injector = builder.injector;
    if (builder.mergeBufferCount == 0) {
      this.conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
          resourceCloser,
          () -> builder.minTopNThreshold
      );
    } else {
      this.conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
          resourceCloser,
          QueryStackTests.getProcessingConfig(true, builder.mergeBufferCount)
      );
    }
    try {
      this.walker = builder.componentSupplier.createQuerySegmentWalker(conglomerate);
    }
    catch (IOException e) {
      throw new RE(e);
    }
    this.resourceCloser.register(walker);

    this.operatorTable = builder.componentSupplier.createOperatorTable();
    this.macroTable = builder.componentSupplier.createMacroTable();

    // also register the static injected mapper, though across multiple test runs
    this.objectMapper = new DefaultObjectMapper().registerModules(
        builder.componentSupplier.getJacksonModules());
    setMapperInjectableValues(builder.componentSupplier.getJacksonInjectables());

    this.qlf = QueryFrameworkUtils.createMockQueryLifecycleFactory(walker, conglomerate);
    this.engine = builder.componentSupplier.createEngine(qlf, objectMapper);
  }

  public final void setMapperInjectableValues(Map<String, Object> injectables)
  {
    // duplicate the injectable values from CalciteTests.INJECTOR initialization, mainly to update the injectable
    // macro table, or whatever else you feel like injecting to a mapper
    LookupExtractorFactoryContainerProvider lookupProvider =
        injector.getInstance(LookupExtractorFactoryContainerProvider.class);
    objectMapper.setInjectableValues(new InjectableValues.Std(injectables)
                                   .addValue(ExprMacroTable.class.getName(), macroTable)
                                   .addValue(ObjectMapper.class.getName(), objectMapper)
                                   .addValue(
                                       DataSegment.PruneSpecsHolder.class,
                                       DataSegment.PruneSpecsHolder.DEFAULT
                                   )
                                   .addValue(
                                       LookupExtractorFactoryContainerProvider.class.getName(),
                                       lookupProvider
                                   )
    );
  }

  public ObjectMapper queryJsonMapper()
  {
    return objectMapper;
  }

  public QueryLifecycle queryLifecycle()
  {
    return qlf.factorize();
  }

  public ExprMacroTable macroTable()
  {
    return macroTable;
  }

  /**
   * Build the statement factory, which also builds all the infrastructure
   * behind the factory by calling methods on this test class. As a result, each
   * factory is specific to one test and one planner config. This method can be
   * overridden to control the objects passed to the factory.
   */
  public SqlStatementFactory statementFactory(
      PlannerConfig plannerConfig,
      AuthConfig authConfig
  )
  {
    final InProcessViewManager viewManager = new InProcessViewManager(DRUID_VIEW_MACRO_FACTORY);
    DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
        injector,
        conglomerate,
        walker,
        plannerConfig,
        viewManager,
        new NoopDruidSchemaManager(),
        authorizerMapper
    );

    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        operatorTable,
        macroTable,
        plannerConfig,
        authorizerMapper,
        objectMapper,
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of())
    );
    final SqlStatementFactory sqlStatementFactory = QueryFrameworkUtils.createSqlStatementFactory(
        engine,
        plannerFactory,
        authConfig
    );

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
    return sqlStatementFactory;
  }

  public void close()
  {
    try {
      resourceCloser.close();
    }
    catch (IOException e) {
      throw new RE(e);
    }
  }
}
