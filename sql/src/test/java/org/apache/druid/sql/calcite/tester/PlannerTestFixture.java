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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryFramework;

import java.io.File;
import java.util.Map;

public class PlannerTestFixture
{
  public static class Builder
  {
    private File resultsDir = new File("target/actual");
    private PlannerConfig plannerConfig = new PlannerConfig();
    private Map<String, Object> defaultQueryContext;
    private Map<String, Object> defaultQueryPlanningContext;
    private AuthConfig authConfig = new AuthConfig();
    private AuthenticationResult defaultAuthResult = CalciteTests.REGULAR_USER_AUTH_RESULT;

    public Builder withPlannerConfig(PlannerConfig plannerConfig)
    {
      this.plannerConfig = plannerConfig;
      return this;
    }

    public Builder withAuthResult(AuthenticationResult authResult)
    {
      this.defaultAuthResult = authResult;
      return this;
    }

    /**
     * Define the query context to use for running the query.
     * Also used for planning, if the planning context is not set.
     *
     * @see #defaultQueryPlanningContext()
     */
    public Builder defaultQueryContext(Map<String, Object> context)
    {
      this.defaultQueryContext = context;
      return this;
    }

    /**
     * Define the query context to use for planning (only).
     *
     * @see #defaultQueryContext()
     */
    public Builder defaultQueryPlanningContext(Map<String, Object> context)
    {
      this.defaultQueryPlanningContext = context;
      return this;
    }

    public PlannerTestFixture build(final QueryFramework framework)
    {
      return new PlannerTestFixture(framework, this);
    }
  }

  private final File resultsDir;
  private final QueryFramework framework;
  private final PlannerConfig plannerConfig;
  private final Map<String, Object> defaultQueryContext;
  private final Map<String, Object> defaultQueryPlanningContext;
  private final AuthConfig authConfig;
  private final AuthenticationResult defaultAuthResult;

  public PlannerTestFixture(final QueryFramework framework, final Builder builder)
  {
    this.framework = framework;
    this.resultsDir = builder.resultsDir;
    this.plannerConfig = builder.plannerConfig;
    this.defaultQueryContext = builder.defaultQueryContext;
    this.defaultQueryPlanningContext = builder.defaultQueryPlanningContext;
    this.authConfig = builder.authConfig;
    this.defaultAuthResult = builder.defaultAuthResult;
  }

  public File resultsDir()
  {
    return resultsDir;
  }

  public PlannerConfig plannerConfig()
  {
    return plannerConfig;
  }

  public AuthenticationResult authResultFor(String user)
  {
    // User mapping is a bit limited: there are only two: the regular user (default)
    // or the super user. The super user is required for tests with an extern data
    // source as the regular user test setup doesn't provide access.
    if (user != null && user.equals(CalciteTests.TEST_SUPERUSER_NAME)) {
      return CalciteTests.SUPER_USER_AUTH_RESULT;
    } else {
      return defaultAuthResult;
    }
  }

  public Map<String, Object> applyDefaultPlanningContext(Map<String, Object> context)
  {
    return defaultQueryPlanningContext == null
        ? context
        : QueryContexts.override(defaultQueryPlanningContext, context);
  }

  public Map<String, Object> applyDefaultContext(Map<String, Object> context)
  {
    return defaultQueryContext == null
        ? context
        : QueryContexts.override(defaultQueryContext, context);
  }

  public SqlStatementFactory statementFactory(PlannerConfig plannerConfig)
  {
    return framework.statementFactory(plannerConfig, authConfig);
  }

  public ObjectMapper jsonMapper()
  {
    return framework.queryJsonMapper();
  }
}
