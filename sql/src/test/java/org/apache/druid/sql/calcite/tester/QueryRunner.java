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

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.DirectStatement.ResultSet;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.planner.CapturedState;
import org.apache.druid.sql.calcite.planner.NoOpCapture;
import org.apache.druid.sql.calcite.planner.PlannerStateCapture;

/**
 * Druid SQL query runner. Encapsulates the planner functionality needed
 * to plan and run a query. Provides the ability to introspect the
 * planner details when testing.
 * <p>
 * This class wraps functionality which was previously spread widely
 * in the code, or tightly coupled to a particular representation. This
 * for is usable for both "production" and test code.
 */
public class QueryRunner
{
  /**
   * Introspected planner details, typically for testing.
   */
  public static class PlanDetails
  {
    private final SqlQueryPlus queryDefn;
    private final ResultSet resultSet;
    private final CapturedState planState;

    public PlanDetails(
        SqlQueryPlus queryDefn,
        CapturedState planState,
        ResultSet resultSet
    )
    {
      this.queryDefn = queryDefn;
      this.planState = planState;
      this.resultSet = resultSet;
    }

    public SqlQueryPlus queryDefn()
    {
      return queryDefn;
    }

    public ResultSet resultSet()
    {
      return resultSet;
    }

    public CapturedState planState()
    {
      return planState;
    }
  }

  private final SqlStatementFactory stmtFactory;

  public QueryRunner(
      SqlStatementFactory stmtFactory
  )
  {
    this.stmtFactory = stmtFactory;
  }

  /**
   * Run a query and provide the result set.
   */
  public Sequence<Object[]> run(SqlQueryPlus defn) throws SqlParseException, ValidationException, RelConversionException
  {
    return plan(defn).run();
  }

  /**
   * Plan the query and provide the planner details for testing.
   */
  public PlanDetails introspectPlan(SqlQueryPlus defn) throws Exception
  {
    CapturedState planState = new CapturedState();
    ResultSet resultSet = plan(defn, planState);
    return new PlanDetails(defn, planState, resultSet);
  }

  /**
   * Plan a query.
   */
  public ResultSet plan(SqlQueryPlus defn) throws SqlParseException, ValidationException, RelConversionException
  {
    return plan(defn, null);
  }

  public ResultSet plan(
      SqlQueryPlus defn,
      PlannerStateCapture planCapture
  ) throws SqlParseException, ValidationException, RelConversionException
  {
    SqlQueryPlus queryPlus = SqlQueryPlus.builder(defn.sql())
        .context(defn.context())
        .parameters(defn.parameters())
        .auth(defn.authResult())
        .build();
    try (DirectStatement stmt = stmtFactory.directStatement(queryPlus)) {
      return stmt.plan(planCapture == null ? NoOpCapture.INSTANCE : planCapture);
    }
  }
}
