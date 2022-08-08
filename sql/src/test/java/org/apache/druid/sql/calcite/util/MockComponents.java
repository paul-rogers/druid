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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.QueryStackTests.MockQueryRunnerFactoryCongomerate;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidCalciteSchemaModule;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.view.DruidViewMacroFactory;
import org.apache.druid.sql.calcite.view.InProcessViewManager;

import java.util.Map;
import java.util.function.Supplier;

public class MockComponents
{
  public static class MockAuthorizerMapper extends AuthorizerMapper
  {
    public MockAuthorizerMapper()
    {
      super(null);
    }

    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        if (authenticationResult.getIdentity().equals(CalciteTests.TEST_SUPERUSER_NAME)) {
          return Access.OK;
        }

        switch (resource.getType()) {
          case ResourceType.DATASOURCE:
            if (resource.getName().equals(CalciteTests.FORBIDDEN_DATASOURCE)) {
              return new Access(false);
            } else {
              return Access.OK;
            }
          case ResourceType.VIEW:
            if (resource.getName().equals("forbiddenView")) {
              return new Access(false);
            } else {
              return Access.OK;
            }
          case ResourceType.QUERY_CONTEXT:
            return Access.OK;
          default:
            return new Access(false);
        }
      };
    }
  }

  public static class MockAuthenticatorMapper extends AuthenticatorMapper
  {
    public MockAuthenticatorMapper()
    {
      super(ImmutableMap.of(
          AuthConfig.ALLOW_ALL_NAME,
          new AllowAllAuthenticator()
          {
            @Override
            public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
            {
              return new AuthenticationResult((String) context.get("user"), AuthConfig.ALLOW_ALL_NAME, null, null);
            }
          }
      ));
    }
  }

  public static class MockComponentsModule implements com.google.inject.Module
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bind(AuthorizerMapper.class).to(MockAuthorizerMapper.class).in(LazySingleton.class);
      binder.bind(AuthenticatorMapper.class).to(MockAuthenticatorMapper.class).in(LazySingleton.class);
      binder.bind(DruidViewMacroFactory.class).to(TestDruidViewMacroFactory.class).in(LazySingleton.class);

      // from DruidCalciteSchemaModule
      binder.bind(String.class).annotatedWith(DruidSchemaName.class).toInstance(DruidCalciteSchemaModule.DRUID_SCHEMA_NAME);
    }
  }

  public static class CalciteTestsViewManager extends InProcessViewManager
  {
    @Inject
    public CalciteTestsViewManager(
        final DruidViewMacroFactory druidViewMacroFactory
    )
    {
      super(druidViewMacroFactory);
    }

    /**
     * Creates the test views. To be called after the injector is set up and
     * the planner factory is available. (The planner factory has this class
     * as a dependency, so this class cannot have planner factory as an
     * inject dependency.
     */
    public void createViews(final PlannerFactory plannerFactory)
    {
      createView(
          plannerFactory,
          "aview",
          "SELECT SUBSTRING(dim1, 1, 1) AS dim1_firstchar FROM foo WHERE dim2 = 'a'"
      );

      createView(
          plannerFactory,
          "bview",
          "SELECT COUNT(*) FROM druid.foo\n"
          + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'"
      );

      createView(
          plannerFactory,
          "cview",
          "SELECT SUBSTRING(bar.dim1, 1, 1) AS dim1_firstchar, bar.dim2 as dim2, dnf.l2 as l2\n"
          + "FROM (SELECT * from foo WHERE dim2 = 'a') as bar INNER JOIN druid.numfoo dnf ON bar.dim2 = dnf.dim2"
      );

      createView(
          plannerFactory,
          "dview",
          "SELECT SUBSTRING(dim1, 1, 1) AS numfoo FROM foo WHERE dim2 = 'a'"
      );

      createView(
          plannerFactory,
          "forbiddenView",
          "SELECT __time, SUBSTRING(dim1, 1, 1) AS dim1_firstchar, dim2 FROM foo WHERE dim2 = 'a'"
      );

      createView(
          plannerFactory,
          "restrictedView",
          "SELECT __time, dim1, dim2, m1 FROM druid.forbiddenDatasource WHERE dim2 = 'a'"
      );

      createView(
          plannerFactory,
          "invalidView",
          "SELECT __time, dim1, dim2, m1 FROM druid.invalidDatasource WHERE dim2 = 'a'"
      );
    }
  }

  /**
   * Like the test-only DruidProcessingConfig created in {@link QueryStackTests#getProcessingConfig},
   * except this one takes its two parameters as properties.
   */
  public static class MockDruidProcessingConfig extends DruidProcessingConfig
  {
    public static final String PROPERTY_BASE = "druid.processing";
    private static final String MERGE_BUFFERS_PROPERTY = "mergeBufferCount";
    public static final String MERGE_BUFFERS_KEY = PROPERTY_BASE + "." + MERGE_BUFFERS_PROPERTY;
    private static final String USE_PARALLEL_MERGE_POOL_PROPERTY = "useParallelMergePool";
    public static final String USE_PARALLEL_MERGE_POOL_KEY = PROPERTY_BASE + "." + USE_PARALLEL_MERGE_POOL_PROPERTY;

    @JsonProperty(MERGE_BUFFERS_PROPERTY)
    private int mergeBufferCount = DEFAULT_NUM_MERGE_BUFFERS;

    @JsonProperty(USE_PARALLEL_MERGE_POOL_PROPERTY)
    private boolean useParallelMergePool;

    @Override
    public String getFormatString()
    {
      return null;
    }

    @Override
    public int intermediateComputeSizeBytes()
    {
      return QueryStackTests.COMPUTE_BUFFER_SIZE;
    }

    @Override
    public int getNumThreads()
    {
      // Only use 1 thread for tests.
      return 1;
    }

    @Override
    public int getNumMergeBuffers()
    {
      if (mergeBufferCount == DEFAULT_NUM_MERGE_BUFFERS) {
        return 2;
      }
      return mergeBufferCount;
    }

    @Override
    public boolean useParallelMergePoolConfigured()
    {
      return useParallelMergePool;
    }
  }

  /**
   * Calcite-specific class that exists only to use the above class which, when
   * created through Guice, has the above class name rather than the base class
   * name.
   */
  public static class CalciteMockQueryRunnerFactoryCongomerate extends MockQueryRunnerFactoryCongomerate
  {
    @Inject
    public CalciteMockQueryRunnerFactoryCongomerate(
        final Closer closer,
        final MockDruidProcessingConfig processingConfig,
        final @Named("minTopNThreshold") Supplier<Integer> minTopNThresholdSupplier
    )
    {
      super(closer, processingConfig, minTopNThresholdSupplier);
    }
  }
}
