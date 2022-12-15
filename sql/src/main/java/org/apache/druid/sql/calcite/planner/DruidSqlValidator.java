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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.BaseDruidSqlValidator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAccessEnum;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.IAE;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Druid extended SQL validator.
 */
class DruidSqlValidator extends BaseDruidSqlValidator
{
  protected DruidSqlValidator(
      SqlOperatorTable opTab,
      CalciteCatalogReader catalogReader,
      JavaTypeFactory typeFactory,
      SqlConformance conformance)
  {
    super(opTab, catalogReader, typeFactory, conformance);
  }

  /**
   * Druid-specific validation for an INSERT statement. In Druid, the columns are
   * matched by name. A datasource, by default, allows the insertion of arbitrary columns,
   * but the catalog may enforce a strict schema (all columns must exist). Destination
   * types are set by the catalog, where available, else by the query.
   */
  @Override
  public void validateInsert(SqlInsert insert)
  {
    // The target namespace is both the target table ID and the row type for that table.
    final SqlValidatorNamespace targetNamespace = getNamespace(insert);
    // Preliminary validation that says we don't know the type of the target table.
    validateNamespace(targetNamespace, unknownType);

    // For now, skip table validation.

    // Validate the query: but we only know how to do SELECT
    final SqlNode source = insert.getSource();
    if (source instanceof SqlSelect) {
      final SqlSelect sqlSelect = (SqlSelect) source;
      validateSelect(sqlSelect, unknownType);
    } else {
      throw new IAE("Druid supports only INSERT ... SELECT");
    }
  }

  public void validateInsert2(SqlInsert insert)
  {
    // The target namespace is both the target table ID and the row type for that table.
    final SqlValidatorNamespace targetNamespace = getNamespace(insert);
    // Preliminary validation that says we don't know the type of the target table.
    validateNamespace(targetNamespace, unknownType);
    // Resolve the table name against the catalog reader.
    final RelOptTable relOptTable = SqlValidatorUtil.getRelOptTable(
        targetNamespace, getCatalogReader().unwrap(Prepare.CatalogReader.class), null, null);
    // Get the table from the above resolution result.
    final SqlValidatorTable table = relOptTable == null
        ? targetNamespace.getTable()
        : relOptTable.unwrap(SqlValidatorTable.class);

    // INSERT has an optional column name list.  If present then
    // reduce the rowtype to the columns specified.  If not present
    // then the entire target rowtype is used.
    final RelDataType targetRowType =
        createTargetRowType(
            table,
            insert.getTargetColumnList(),
            false);

    final SqlNode source = insert.getSource();
    if (source instanceof SqlSelect) {
      final SqlSelect sqlSelect = (SqlSelect) source;
      validateSelect(sqlSelect, targetRowType);
    } else {
      final SqlValidatorScope scope = scopes.get(source);
      validateQuery(source, scope, targetRowType);
    }

    // REVIEW jvs 4-Dec-2008: In FRG-365, this namespace row type is
    // discarding the type inferred by inferUnknownTypes (which was invoked
    // from validateSelect above).  It would be better if that information
    // were used here so that we never saw any untyped nulls during
    // checkTypeAssignment.
    final RelDataType sourceRowType = getNamespace(source).getRowType();
    final RelDataType logicalTargetRowType =
        getLogicalTargetRowType(targetRowType, insert);
    setValidatedNodeType(insert, logicalTargetRowType);
    final RelDataType logicalSourceRowType =
        getLogicalSourceRowType(sourceRowType, insert);

    final List<ColumnStrategy> strategies =
        table.unwrap(RelOptTable.class).getColumnStrategies();

    final RelDataType realTargetRowType = typeFactory.createStructType(
        logicalTargetRowType.getFieldList()
            .stream().filter(f -> strategies.get(f.getIndex()).canInsertInto())
            .collect(Collectors.toList()));

    final RelDataType targetRowTypeToValidate =
        logicalSourceRowType.getFieldCount() == logicalTargetRowType.getFieldCount()
        ? logicalTargetRowType
        : realTargetRowType;

    // Skip the virtual columns(can not insert into) type assignment
    // check if the source fields num is equals with
    // the real target table fields num, see how #checkFieldCount was used.
    checkTypeAssignment(logicalSourceRowType, targetRowTypeToValidate, insert);

    // Refresh the insert row type to keep sync with source.
    setValidatedNodeType(insert, targetRowTypeToValidate);
  }
}
