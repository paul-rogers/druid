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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.BaseDruidSqlValidator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlScopedShuttle;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.Util;
import org.apache.druid.sql.calcite.planner.DruidMetadataResolver;
import org.apache.druid.sql.calcite.table.DatasourceTable;

/**
 * Druid extended SQL validator. (At present, it doesn't actually
 * have any extensions yet, but it will soon.)
 */
class DruidSqlValidator extends BaseDruidSqlValidator
{
  private final DruidMetadataResolver metadataResolver;

  public DruidSqlValidator(
      SqlOperatorTable opTab,
      CalciteCatalogReader catalogReader,
      JavaTypeFactory typeFactory,
      SqlConformance conformance,
      DruidMetadataResolver metadataResolver
  )
  {
    super(opTab, catalogReader, typeFactory, conformance);
    this.metadataResolver = metadataResolver;
  }

  @Override
  public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
    final Expander expander = new Expander(this, scope);
    SqlNode newExpr = expr.accept(expander);
    if (expr != newExpr) {
      setOriginal(newExpr, expr);
    }
    return newExpr;
  }

  /**
   * Converts an expression into canonical form by fully-qualifying any
   * identifiers.
   * Clone of {@code SqlValidatorImp.Expander} with measure handling
   * added. The Calcite class is private, so we can't extend it.
   */
  private static class Expander extends SqlScopedShuttle
  {
    protected final DruidSqlValidator validator;
    private final boolean hasGroupBy;
    private boolean inAgg;

    Expander(DruidSqlValidator validator, SqlValidatorScope scope)
    {
      super(scope);
      this.validator = validator;
      if (scope instanceof SelectScope) {
        this.hasGroupBy = ((SelectScope) scope).getNode().getGroup() != null;
      } else {
        this.hasGroupBy = false;
      }
    }

    @Override
    public SqlNode visit(SqlIdentifier id)
    {
      // First check for builtin functions which don't have
      // parentheses, like "LOCALTIME".
      final SqlCall call = validator.makeNullaryCall(id);
      if (call != null) {
        return call.accept(this);
      }
      final SqlQualified qual = getScope().fullyQualify(id);
      SqlNode expandedExpr = expandColumn(id, qual);
      if (expandedExpr == null) {
        final SqlIdentifier fqId = qual.identifier;
        expandedExpr = expandDynamicStar(id, fqId);
      }
      validator.setOriginal(expandedExpr, id);
      return expandedExpr;
    }

    /**
     * Expand a datasource column when metadata is available. For measures,
     * if the query has a GROUP BY, and the measure appears outside of an
     * aggregate function, then rewrite measure {@code m} to be
     * {@code AGG(m)} where {@code AGG} is the SQL version of the reduction
     * function used for the measure. Will not rewrite if the column is other
     * than a measure, or if the measure is for an aggregate that does not
     * have a single aggregate function.
     */
    private SqlNode expandColumn(SqlIdentifier origId, SqlQualified qual)
    {
      if (!hasGroupBy || inAgg) {
        return null;
      }
      if (!(qual.namespace instanceof IdentifierNamespace)) {
        return null;
      }
      SqlValidatorTable validatorTable = ((IdentifierNamespace) qual.namespace).getTable();
      if (validatorTable == null || !(validatorTable instanceof RelOptTableImpl)) {
        return null;
      }
      RelOptTableImpl relTable = (RelOptTableImpl) validatorTable;
      DatasourceTable table = relTable.unwrap(DatasourceTable.class);
      if (table == null) {
        return null;
      }
      return table.rewriteSelectColumn(
          origId.getParserPosition(),
          qual.identifier,
          validator.metadataResolver
      );
    }

    @Override
    protected SqlNode visitScoped(SqlCall call)
    {
      switch (call.getKind()) {
        case SCALAR_QUERY:
        case CURRENT_VALUE:
        case NEXT_VALUE:
        case WITH:
          return call;
        default:
      }
      boolean oldInAgg = inAgg;
      if (SqlKind.AGGREGATE.contains(call.getKind())) {
        inAgg = true;
      }
      // Only visits arguments which are expressions. We don't want to
      // qualify non-expressions such as 'x' in 'empno * 5 AS x'.
      ArgHandler<SqlNode> argHandler =
          new CallCopyingArgHandler(call, false);
      call.getOperator().acceptCall(this, call, true, argHandler);
      final SqlNode result = argHandler.result();
      validator.setOriginal(result, call);
      inAgg = oldInAgg;
      return result;
    }

    protected SqlNode expandDynamicStar(SqlIdentifier id, SqlIdentifier fqId)
    {
      if (DynamicRecordType.isDynamicStarColName(Util.last(fqId.names))
          && !DynamicRecordType.isDynamicStarColName(Util.last(id.names))) {
        // Convert a column ref into ITEM(*, 'col_name')
        // for a dynamic star field in dynTable's rowType.
        SqlNode[] inputs = new SqlNode[2];
        inputs[0] = fqId;
        inputs[1] = SqlLiteral.createCharString(
            Util.last(id.names),
            id.getParserPosition());
        return new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            inputs,
            id.getParserPosition());
      }
      return fqId;
    }
  }
}
