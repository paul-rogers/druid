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

package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.RowSchema.ColumnSchema;
import org.apache.druid.exec.batch.impl.ConstantScalarReader;
import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.exec.plan.WindowSpec.CopyProjection;
import org.apache.druid.exec.plan.WindowSpec.OffsetExpression;
import org.apache.druid.exec.plan.WindowSpec.OutputColumn;
import org.apache.druid.exec.plan.WindowSpec.SimpleExpression;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectionBuilder
{
  private final BatchBuffer buffer;
  private final WindowSpec spec;
  private final ExprMacroTable macroTable;
  private final Map<Integer, BatchReader> offsetReaders = new HashMap<>();
  private final SchemaBuilder schemaBuilder = new SchemaBuilder();
  private BatchReader primaryReader;
  private List<ScalarColumnReader> colReaders;

  public ProjectionBuilder(BatchBuffer buffer, final ExprMacroTable macroTable, WindowSpec spec)
  {
    this.buffer = buffer;
    this.spec = spec;
    this.macroTable = macroTable;
    primaryReader = buffer.inputSchema.newReader();
    buildOffsetReaders();
    int rowWidth = spec.columns.size();
    colReaders = new ArrayList<>(rowWidth);
    for (int i = 0; i < rowWidth; i++) {
      colReaders.add(buildProjection(spec.columns.get(i)));
    }
  }

  public BatchBuffer buffer()
  {
    return buffer;
  }

  public RowSchema schema()
  {
    return schemaBuilder.build();
  }

  public List<ScalarColumnReader> columnReaders()
  {
    return colReaders;
  }

  public BatchReader primaryReader()
  {
    return primaryReader;
  }

  public Map<Integer, BatchReader> offsetReaders()
  {
    return offsetReaders;
  }

  private void buildOffsetReaders()
  {
    for (OutputColumn col : spec.columns) {
      if (!(col instanceof OffsetExpression)) {
        continue;
      }
      OffsetExpression offsetProj = (OffsetExpression) col;
      offsetReaders.computeIfAbsent(offsetProj.offset, n -> buffer.inputSchema.newReader());
    }
  }

  private ScalarColumnReader buildProjection(OutputColumn outputColumn)
  {
    ColumnSchema colSchema = schemaBuilder.addScalar(outputColumn.name, outputColumn.dataType);
    if (outputColumn instanceof CopyProjection) {
      return primaryReader.columns().scalar(((CopyProjection) outputColumn).name);
    } else if (outputColumn instanceof SimpleExpression) {
      return buildSimpleExpression((SimpleExpression) outputColumn, colSchema);
    } else if (outputColumn instanceof OffsetExpression) {
      return buildOffsetExpression((OffsetExpression) outputColumn, colSchema);
    }
    throw new UOE("Invalid output column type: [%s]", outputColumn.getClass().getSimpleName());
  }

  private ScalarColumnReader buildSimpleExpression(SimpleExpression exprCol, ColumnSchema colSchema)
  {
    Expr expr = Parser.parse(exprCol.expression, macroTable);
    if (expr.isNullLiteral()) {
      return new ConstantScalarReader(colSchema, null);
    } else if (expr.isLiteral()) {
      return new ConstantScalarReader(colSchema, expr.getLiteralValue());
    } else if (expr.isIdentifier()) {
      // Not really legal: no point in copying with lead/lag 0.
      ScalarColumnReader sourceCol = primaryReader.columns().scalar(expr.getIdentifierIfIdentifier());
      Preconditions.checkNotNull(sourceCol);
      return sourceCol;
    }
    throw new UOE("Invalid expression: [%s]", exprCol.expression);
  }

  private ScalarColumnReader buildOffsetExpression(OffsetExpression exprCol, ColumnSchema colSchema)
  {
    Expr expr = Parser.parse(exprCol.expression, macroTable);
    if (expr.isNullLiteral()) {
      // Should not happen if the planner is doing the right thing.
      return new ConstantScalarReader(colSchema, null);
    } else if (expr.isLiteral()) {
      // Should not happen if the planner is doing the right thing.
      return new ConstantScalarReader(colSchema, expr.getLiteralValue());
    } else if (expr.isIdentifier()) {
      BatchReader sourceReader = offsetReaders.get(exprCol.offset);
      ScalarColumnReader sourceCol = sourceReader.columns().scalar(expr.getIdentifierIfIdentifier());
      Preconditions.checkNotNull(sourceCol);
      return sourceCol;
    }
    throw new UOE("Invalid expression: [%s]", exprCol.expression);
  }
}
