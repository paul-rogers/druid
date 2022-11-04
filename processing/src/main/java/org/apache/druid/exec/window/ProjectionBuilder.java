package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
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
import org.apache.druid.exec.window.WindowFrameCursor.PrimaryCursor;
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
  private final Map<Integer, WindowFrameCursor> offsetReaders = new HashMap<>();
  private final SchemaBuilder schemaBuilder = new SchemaBuilder();
  private WindowFrameCursor primaryReader;
  private List<ScalarColumnReader> colReaders;

  public ProjectionBuilder(BatchBuffer buffer, final ExprMacroTable macroTable, WindowSpec spec)
  {
    this.buffer = buffer;
    this.spec = spec;
    this.macroTable = macroTable;
  }

  public WindowFrameSequencer build()
  {
    primaryReader = new PrimaryCursor(buffer);
    buildOffsetReaders();
    int rowWidth = spec.columns.size();
    colReaders = new ArrayList<>(rowWidth);
    for (int i = 0; i < rowWidth; i++) {
      colReaders.add(buildProjection(spec.columns.get(i)));
    }
    return new WindowFrameSequencer(buffer, primaryReader, offsetReaders.values());
  }

  public RowSchema schema()
  {
    return schemaBuilder.build();
  }

  public List<ScalarColumnReader> columnReaders()
  {
    return colReaders;
  }

  private void buildOffsetReaders()
  {
    for (OutputColumn col : spec.columns) {
      if (!(col instanceof OffsetExpression)) {
        continue;
      }
      OffsetExpression offsetProj = (OffsetExpression) col;
      offsetReaders.computeIfAbsent(offsetProj.offset, n -> WindowFrameCursor.offsetCursor(buffer, n));
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
      WindowFrameCursor sourceReader = offsetReaders.get(exprCol.offset);
      ScalarColumnReader sourceCol = sourceReader.columns().scalar(expr.getIdentifierIfIdentifier());
      Preconditions.checkNotNull(sourceCol);
      return sourceCol;
    }
    throw new UOE("Invalid expression: [%s]", exprCol.expression);
  }
}
