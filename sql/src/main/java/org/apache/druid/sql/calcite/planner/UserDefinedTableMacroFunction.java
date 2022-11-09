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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.java.util.common.UOE;

import java.util.List;

/**
 * Table macro designed for use with the Druid EXTEND
 * operator. Example:
 * <code><pre>
 * INSERT INTO dst
 * SELECT *
 * FROM TABLE(staged(
 *    source => 'inline',
 *    format => 'csv',
 *    data => 'a,b,1
 * c,d,2
 * '
 *   ))
 *   EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
 * PARTITIONED BY ALL TIME
 * </pre></code>
 * <p>
 * Calcite supports the Apache Phoenix EXTEND operator of the form:
 * <code><pre>
 * SELECT ..
 * FROM myTable EXTEND (x VARCHAR, ...)
 * </pre></code>
 * Though, oddly, a search of Apache Phoenix itself does not find
 * a hit for EXTEND, so perhaps the feature was never completed?
 * <p>
 * For Druid, we want the above form: extend a table function, not a
 * literal table. Since we can't change the Calcite parser, we instead use
 * tricks to within the constraints of the parser.
 * <ul>
 * <li>First, use use a Python script to modify the parser to add the
 * EXTEND rule for a table function.</li>
 * <li>Calcite expects the EXTEND operator to have two arguments: an identifier
 * and the column list. Since our case has a function call as the first argument,
 * we can't let Calcite see our AST. So, we use a rewrite trick to convert the
 * EXTEND node into the usual TABLE(.) node, and we modify the associated macro
 * to hold onto the schema, which is not out of sight of Calcite, and so will not
 * cause problems with rules that don't understand our usage.</li>
 * <li>Calcite will helpfully rewrite calls, replacing our modified operator with
 * the original. So, we override those to keep our modified operator.</li>
 * <li>When asked to produce a table ({@code apply(.)}), we call a Druid-specific
 * version that passes along the schema saved previously.</li>
 * <li>The extended {@link DruidTableMacro} uses the schema to define the
 * input source.</li>
 * <li>Care is taken that the same {@code DruidTableMacro} can be used without
 * EXTEND. In this case, the schema will be empty and the input source must have
 * a way of providing the schema. The batch ingest feature does not yet support
 * this use case, but it seems a reasonable extension. Example: CSV that has a
 * header row, or a "classic" lookup table that, by definition, has only two
 * columns.</li>
 * </ul>
 * <p>
 * Note that unparsing is a bit of a nuisance. Our trick places the EXTEND
 * list in the wrong place, and we'll unparse SQL as:
 * <code><pre>
 * FROM TABLE(fn(arg1, arg2) EXTEND (x VARCHAR, ...))
 * </pre></code>
 * Since we seldom use unparse, we can perhaps live with this limitation for now.
 */
public abstract class UserDefinedTableMacroFunction extends SqlUserDefinedTableMacro
{
  public UserDefinedTableMacroFunction(
      SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      TableMacro tableMacro)
  {
    super(opName, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes, tableMacro);
  }

  public UserDefinedTableMacroFunction copyWithSchema(SqlNodeList schema)
  {
    throw new UOE("Implement copyWithSchema to use this macro with EXTEND");
  }

  public SqlBasicCall rewriteCall(SqlBasicCall oldCall, SqlNodeList schema)
  {
    UserDefinedTableMacroFunction newOp = copyWithSchema(schema);
    return new ExtendedCall(oldCall, newOp, schema);
  }

  private static class ExtendedCall extends SqlBasicCall
  {
    private final SqlNodeList schema;

    public ExtendedCall(SqlBasicCall oldCall, UserDefinedTableMacroFunction macro, SqlNodeList schema)
    {
      super(
          macro,
          oldCall.getOperands(),
          oldCall.getParserPosition(),
          false,
          oldCall.getFunctionQuantifier());
      this.schema = schema;
    }

    public ExtendedCall(ExtendedCall from, SqlParserPos pos)
    {
      super(
          from.getOperator(),
          from.getOperands(),
          pos,
          false,
          from.getFunctionQuantifier());
      this.schema = from.schema;
    }

    /**
     * Politely decline to revise the operator: we want the one we
     * constructed to hold the schema, not the one re-resolved during
     * validation.
     */
    @Override
    public void setOperator(SqlOperator operator)
    {
      if (getOperator() == null) {
        super.setOperator(operator);
      }
    }

    @Override
    public SqlNode clone(SqlParserPos pos)
    {
      return new ExtendedCall(this, pos);
    }

    @Override
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
      super.unparse(writer, leftPrec, rightPrec);
      writer.keyword("EXTEND");
      Frame frame = writer.startList("(", ")");
      schema.unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
    }
  }
}
