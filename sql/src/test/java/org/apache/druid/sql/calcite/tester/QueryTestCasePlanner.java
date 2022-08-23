package org.apache.druid.sql.calcite.tester;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlInsert;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.DirectStatement.ResultSet;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.CapturedState;
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

import org.apache.druid.sql.calcite.rel.DruidRel;

import java.util.List;

public class QueryTestCasePlanner
{
  public static final Logger log = new Logger(QueryTestCasePlanner.class);

  private final PlannerTestFixture plannerFixture;
  private final SqlStatementFactory statementFactory;
  private final QueryTestCase testCase;
  private final ActualResults results;
  private final SqlQueryPlus queryPlus;
  private CapturedState planState;
  private ResultSet resultSet;

  public QueryTestCasePlanner(
      PlannerTestFixture plannerFixture,
      SqlStatementFactory stmtFactory,
      QueryTestCase testCase,
      ActualResults results
  )
  {
    this.plannerFixture = plannerFixture;
    this.testCase = testCase;
    this.results = results;
    this.statementFactory = stmtFactory;
    this.queryPlus = SqlQueryPlus
        .builder(testCase.sql())
        // Plan with only the context in the test case. Ensures that the
        // case with no extra context works. Makes native queries smaller.
        .context(plannerFixture.applyDefaultPlanningContext(testCase.context()))
        .sqlParameters(testCase.parameters())
        .auth(plannerFixture.authResultFor(testCase.user()))
        .build();
  }

  protected ActualResults run()
  {
    gatherResults();
    results.verify();
    if (resultSet != null) {
      resultSet.closeQuietly();
    }
    return results;
  }

  // Lazy planning evaluation in case the test only wants to EXPLAIN,
  // but not capture detail plan results.
  private void preparePlan() throws Exception
  {
    if (planState != null) {
      return;
    }
    try (DirectStatement stmt = statementFactory.directStatement(queryPlus)) {
      planState = new CapturedState();
      resultSet = stmt.plan(planState);
    }
  }

  private void gatherResults()
  {
    try {
      // Planning is done on demand. If we should fail in planning,
      // go ahead and try now. If the query succeeds, no need to try
      // the other items as success and failure are mutually exclusive.
      if (testCase.shouldFail()) {
        preparePlan();
        return;
      }

      // Gather actual plan results to compare against expected values.
      gatherParseTree();
      gatherUnparse();
      gatherSchema();
      gatherPlan();
      gatherNativeQuery();
      gatherResources();
      gatherTargetSchema();
      gatherExplain();
      gatherExecPlan();
    }
    catch (Exception e) {
      results.exception(e);
      return;
    }
  }

  private void gatherParseTree() throws Exception
  {
    PatternSection ast = testCase.ast();
    if (ast == null) {
      return;
    }
    preparePlan();
    ParseTreeVisualizer visitor = new ParseTreeVisualizer();
    planState.sqlNode.accept(visitor);
    String output = visitor.result();
    results.ast(ast, output);
  }

  private void gatherUnparse() throws Exception
  {
    PatternSection testSection = testCase.unparsed();
    if (testSection == null) {
      return;
    }
    preparePlan();
    String unparsed = planState.sqlNode.toString();
    results.unparsed(testSection, unparsed);
  }

  private void gatherPlan() throws Exception
  {
    PatternSection testSection = testCase.plan();
    if (testSection == null) {
      return;
    }
    preparePlan();
    if (planState.bindableRel != null) {
      gatherBindablePlan(testSection);
    } else if (planState.relRoot != null) {
      gatherDruidPlan(testSection);
    } else {
      throw new ISE(
          StringUtils.format(
              "Test case [%s] has a plan but the planner did not produce one.",
              testCase.label()));
    }
  }

  private void gatherDruidPlan(PatternSection testSection)
  {
    // Do-it-ourselves plan since the actual plan omits insert.
    String queryPlan = RelOptUtil.dumpPlan(
        "",
        planState.relRoot.rel,
        SqlExplainFormat.TEXT,
        SqlExplainLevel.DIGEST_ATTRIBUTES);
    String plan;
    SqlInsert insertNode = planState.insertNode;
    if (insertNode == null) {
      plan = queryPlan;
    } else if (insertNode instanceof DruidSqlInsert) {
      DruidSqlInsert druidInsertNode = (DruidSqlInsert) insertNode;
      // The target is a SQLIdentifier literal, pre-resolution, so does
      // not include the schema.
      plan = StringUtils.format(
          "LogicalInsert(target=[%s], granularity=[%s])\n",
          druidInsertNode.getTargetTable(),
          druidInsertNode.getPartitionedBy() == null ? "<none>" : druidInsertNode.getPartitionedBy());
      if (druidInsertNode.getClusteredBy() != null) {
        plan += "  Clustered By: " + druidInsertNode.getClusteredBy();
      }
      plan +=
          "  " + StringUtils.replace(queryPlan, "\n ", "\n   ");
    } else if (insertNode instanceof DruidSqlReplace) {
      DruidSqlReplace druidInsertNode = (DruidSqlReplace) insertNode;
      // The target is a SQLIdentifier literal, pre-resolution, so does
      // not include the schema.
      plan = StringUtils.format(
          "LogicalInsert(target=[%s], granularity=[%s])\n",
          druidInsertNode.getTargetTable(),
          druidInsertNode.getPartitionedBy() == null ? "<none>" : druidInsertNode.getPartitionedBy());
      if (druidInsertNode.getClusteredBy() != null) {
        plan += "  Clustered By: " + druidInsertNode.getClusteredBy();
      }
      plan +=
          "  " + StringUtils.replace(queryPlan, "\n ", "\n   ");
    } else {
      plan = queryPlan;
    }
    results.plan(testSection, plan);
  }

  private void gatherBindablePlan(PatternSection testSection)
  {
    String queryPlan = RelOptUtil.dumpPlan(
        "",
        planState.bindableRel,
        SqlExplainFormat.TEXT,
        SqlExplainLevel.DIGEST_ATTRIBUTES);
    results.plan(testSection, queryPlan);
  }

  private void gatherExecPlan()
  {
    PatternSection testSection = testCase.execPlan();
    if (testSection == null) {
      return;
    }
    results.execPlan(testSection,
        QueryTestCases.formatJson(
            plannerFixture.jsonMapper(),
            planState.execPlan));
  }

  private void gatherNativeQuery() throws Exception
  {
    PatternSection testSection = testCase.nativeQuery();
    if (testSection == null) {
      return;
    }
    preparePlan();
    DruidRel<?> druidRel = planState.druidRel;
    if (druidRel == null) {
      throw new ISE(
          StringUtils.format(
              "Test case [%s] has a native query but the planner did not produce one.",
              testCase.label()));
    }
    results.nativeQuery(
        testSection,
        QueryTestCases.serializeDruidRel(plannerFixture.jsonMapper(), druidRel));
  }

  private void gatherSchema() throws Exception
  {
    PatternSection section = testCase.schema();
    if (section == null) {
      return;
    }
    preparePlan();
    results.schema(
        section,
        QueryTestCases.formatSchema(resultSet.rowType()));
  }

  private void gatherResources() throws Exception
  {
    ResourcesSection section = testCase.resourceActions();
    if (section == null) {
      return;
    }
    preparePlan();
    results.resourceActions(
        section,
        planState.resources);
  }

  private void gatherTargetSchema() throws Exception
  {
    PatternSection section = testCase.targetSchema();
    if (section == null) {
      return;
    }
    preparePlan();
    if (planState.insertNode == null) {
      results.errors().add(
          StringUtils.format(
              "Query [%s] expects a target schema, but the SQL is not an INSERT statement.",
              testCase.label()));
      return;
    }

    List<RelDataTypeField> fields = planState.relRoot.validatedRowType.getFieldList();
    String[] actual = new String[fields.size()];
    for (int i = 0; i < actual.length; i++) {
      RelDataTypeField field = fields.get(i);
      actual[i] = field.getName() + " " + field.getType();
    }
    results.targetSchema(section, actual);
  }

  private void gatherExplain() throws Exception
  {
    PatternSection testSection = testCase.explain();
    if (testSection == null) {
      return;
    }
    try (DirectStatement stmt = statementFactory.directStatement(queryPlus)) {
      Object[] row = stmt.execute().toList().get(0);
      results.explain(
          testSection,
          QueryTestCases.formatExplain(
              plannerFixture.jsonMapper(),
              (String) row[0],
              (String) row[1]
          )
      );
    }
  }
}
