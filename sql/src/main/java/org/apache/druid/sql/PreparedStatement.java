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

package org.apache.druid.sql;

import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PrepareResult;

import java.util.List;

/**
 * Statement for the JDBC prepare-once, execute many model.
 */
public class PreparedStatement extends AbstractStatement
{
  private final SqlRequest originalRequest;
  private PrepareResult prepareResult;

  public PreparedStatement(
      final SqlToolbox lifecycleToolbox,
      final SqlRequest sqlRequest
  )
  {
    super(lifecycleToolbox, sqlRequest, null);
    this.originalRequest = sqlRequest;
  }

  /**
   * Prepare the query lifecycle for execution, without completely planning into
   * something that is executable, but including some initial parsing and
   * validation, to support prepared statements via JDBC.
   * <p>
   * Note that, per JDBC convention, the prepare step does not provide
   * parameter values: those are provided later during execution and will generally
   * vary from one execution to the next.
   *
   * <ul>
   * <li>Create the planner.</li>
   * <li>Parse the statement.</li>
   * <li>JDBC does not provide parameter values at prepare time.
   * They are provided during execution later, where we'll replan the
   * query to use the <a href="https://github.com/apache/druid/pull/6974">
   * "query optimized"</a> structure.</li>
   * <li>Validate the query against the Druid catalog.</li>
   * <li>Authorize access to the resources which the query needs.</li>
   * <li>Return a {@link PrepareResult} which describes the query.</li>
   * </ul>
   */
  public PrepareResult prepare()
  {
    try (DruidPlanner planner = sqlToolbox.plannerFactory.createPlanner(
        sqlRequest.sql(),
        sqlRequest.context())) {
      validate(planner);
      authorize(
          planner,
          authorizer());

      // Do the prepare step.
      try {
        this.prepareResult = planner.prepare();
        return prepareResult;
      }
      catch (ValidationException e) {
        throw new SqlPlanningException(e);
      }
    }
  }

  /**
   * Execute a prepared JDBC query. Druid uses
   * <a href="https://github.com/apache/druid/pull/6974">
   * "query optimized"</a> parameters, which means we do not reuse the statement
   * prepared above, but rather plan anew with the actual parameter values. The
   * same statement can be execute many times, including concurrently. It is
   * easier and safer to create a new planner for each execution than to try to
   * share the parse tree across executions.
   */
  public DirectStatement execute(List<TypedValue> parameters)
  {
    return new DirectStatement(
        sqlToolbox,
        originalRequest.withParameters(parameters)
        );
  }
}
