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

package org.apache.druid.sql.calcite.external;

import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.UserDefinedTableMacroFunction;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Calcite function, which holds a {@link TableMacro} for an input table. The macro
 * is assumed to be {@link org.apache.druid.sql.calcite.external.InputTableMacro InputTableMacro},
 * though that is not required.
 * <p>
 * Computes the parameter types from the macro's parameters. Marks the list as
 * variadic since {@code InputTableMacro} has many optional parameters.
 */
public class InputTableMacroFunction extends UserDefinedTableMacroFunction implements AuthorizableOperator
{
  public InputTableMacroFunction(String name, TableMacro macro)
  {
    super(
        new SqlIdentifier(Collections.singletonList(name), SqlParserPos.ZERO),
        ReturnTypes.CURSOR,
        null,
        // Use our own definition of variadic since Calcite's doesn't allow
        // optional parameters.
        variadic(macro.getParameters()),
        macro.getParameters()
             .stream()
             .map(parameter -> parameter.getType(DruidTypeSystem.TYPE_FACTORY))
             .collect(Collectors.toList()),
        macro);
  }

  @Override
  public Set<ResourceAction> computeResources(final SqlCall call)
  {
    return Collections.singleton(ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION);
  }

  /**
   * Define a variadic (variable arity) type checker that allows an argument
   * count that ranges from the number of required parameters to the number of
   * available parameters. We have to define this because the Calcite form does
   * not allow optional parameters, but we allow any parameter to be optional.
   * We are also not fussy about the type: we catch any type errors from the
   * declared types. We catch missing required parameters at conversion time,
   * where we also catch invalid values, incompatible values, and so on.
   */
  public static SqlOperandTypeChecker variadic(List<FunctionParameter> params)
  {
    int min = 0;
    for (FunctionParameter param : params) {
      if (!param.isOptional()) {
        min++;
      }
    }
    SqlOperandCountRange range = SqlOperandCountRanges.between(min, params.size());
    return new SqlOperandTypeChecker()
    {
      @Override
      public boolean checkOperandTypes(
          SqlCallBinding callBinding,
          boolean throwOnFailure)
      {
        return range.isValidCount(callBinding.getOperandCount());
      }

      @Override
      public SqlOperandCountRange getOperandCountRange()
      {
        return range;
      }

      @Override
      public String getAllowedSignatures(SqlOperator op, String opName)
      {
        return opName + "(...)";
      }

      @Override
      public boolean isOptional(int i)
      {
        return true;
      }

      @Override
      public Consistency getConsistency()
      {
        return Consistency.NONE;
      }
    };
  }
}
