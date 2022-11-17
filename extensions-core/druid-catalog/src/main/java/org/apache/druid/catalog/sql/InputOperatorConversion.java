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

package org.apache.druid.catalog.sql;

import com.google.inject.Inject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.external.ExternalTableMacro;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Registers the "INPUT" operator, which is used in queries like
 * "INSERT INTO dst SELECT * FROM TABLE(INPUT(tableName, ...))".
 *
 * This class is exercised in CalciteInsertDmlTest but is not currently exposed to end users.
 *
 * Similar to {@link ExternalOperatorConversion},
 * but for input sources defined in the catalog. Uses the same resource for security.
 */
public class InputOperatorConversion implements SqlOperatorConversion
{
  public static final String FUNCTION_NAME = "INPUT";

  private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);

  private final SqlUserDefinedTableMacro operator;

  @Inject
  public InputOperatorConversion(final ExternalTableMacro macro)
  {
    this.operator = new InputOperator(macro);
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    return null;
  }

  private static class InputOperator extends SqlUserDefinedTableMacro implements AuthorizableOperator
  {
    public InputOperator(final ExternalTableMacro macro)
    {
      super(
          new SqlIdentifier(FUNCTION_NAME, SqlParserPos.ZERO),
          ReturnTypes.CURSOR,
          null,
          OperandTypes.sequence(
              "(table)",
              OperandTypes.family(SqlTypeFamily.CURSOR)
          ),
          macro.getParameters()
               .stream()
               .map(parameter -> parameter.getType(TYPE_FACTORY))
               .collect(Collectors.toList()),
          macro
      );
    }

    @Override
    public Set<ResourceAction> computeResources(final SqlCall call)
    {
      return Collections.singleton(ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION);
    }
  }
}
