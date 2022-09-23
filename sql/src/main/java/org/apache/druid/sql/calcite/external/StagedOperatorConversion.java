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

import com.google.inject.Inject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.catalog.model.ColumnSchema;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.UserDefinedTableMacroFunction;
import org.apache.druid.sql.catalog.CatalogConversions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Registers the "STAGED" operator, which is used in queries like
 * "INSERT INTO dst SELECT * FROM TABLE(STAGED(arg => value, ...))".
 */
public class StagedOperatorConversion implements SqlOperatorConversion
{
  public static final String FUNCTION_NAME = "STAGED";

  private final SqlUserDefinedTableMacro operator;

  @Inject
  public StagedOperatorConversion(
      final InputTableMacro macro
  )
  {
    this.operator = new StagedTableMacroFunction(FUNCTION_NAME, macro);
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

  /**
   * The Calcite function (operator) which holds the actual macro. This form works with
   * the EXTEND functionality to capture the provided schema as a hidden member of the
   * function. This function is static (there is only one in the system), but gives
   * rise to a table-specific function that embeds the schema.
   */
  private static class StagedTableMacroFunction extends InputTableMacroFunction
  {
    protected final TableMacro macro;

    public StagedTableMacroFunction(String name, TableMacro macro)
    {
      super(name, macro);

      // Because Calcite's copy of the macro is private
      this.macro = macro;
    }

    @Override
    public UserDefinedTableMacroFunction copyWithSchema(SqlNodeList schema)
    {
      return new InputTableMacroFunction(
          getName(),
          new ExtendedTableMacro((InputTableMacro) macro, schema));
    }
  }

  /**
   * Calcite table macro created dynamically to squirrel away the
   * schema provided by the EXTEND clause to allow <pre><code>
   * SELECT ... FROM TABLE(fn(arg => value, ...)) (col1 <type1>, ...)
   * </code></pre>
   * This macro wraps the actual input table macro, which does the
   * actual work to build the Druid table.
   */
  private static class ExtendedTableMacro implements TableMacro
  {
    private final InputTableMacro delegate;
    private final SqlNodeList schema;

    public ExtendedTableMacro(InputTableMacro delegate, SqlNodeList schema)
    {
      this.delegate = delegate;
      this.schema = schema;
    }

    @Override
    public TranslatableTable apply(List<Object> arguments)
    {
      // Converts from a list of (identifier, type, ...) pairs to
      // a Druid column schema object. The schema itself comes from the
      // Druid-specific EXTEND syntax added to the parser.
      List<ColumnSchema> cols = new ArrayList<>();
      for (int i = 0; i < schema.size(); i += 2) {
        cols.add(CatalogConversions.toColumnSchema(
            (SqlIdentifier) schema.get(i),
            (SqlDataTypeSpec) schema.get(i + 1)));
      }
      return delegate.apply(
          delegate.model().argsFromCalcite(arguments),
          cols);
    }

    @Override
    public List<FunctionParameter> getParameters()
    {
      return delegate.getParameters();
    }
  }
}
