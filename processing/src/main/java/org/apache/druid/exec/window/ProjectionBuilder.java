package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.batch.RowReader.RowCursor;
import org.apache.druid.exec.batch.RowSchema.ColumnSchema;
import org.apache.druid.exec.batch.impl.ConstantScalarReader;
import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.exec.plan.WindowSpec.OffsetExpression;
import org.apache.druid.exec.plan.WindowSpec.OutputColumn;
import org.apache.druid.exec.plan.WindowSpec.SimpleExpression;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.exec.window.BatchBuffer.PartitionCursor;
import org.apache.druid.exec.window.BatchBuffer.PartitionReader;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProjectionBuilder
{
  private final BatchBuffer buffer;
  private final WindowSpec spec;
  private final ExprMacroTable macroTable;
  private final Map<Integer, PartitionReader> offsetReaders = new HashMap<>();
  private final SchemaBuilder schemaBuilder = new SchemaBuilder();
  private PartitionReader primaryReader;

  public ProjectionBuilder(BatchBuffer buffer, final ExprMacroTable macroTable, WindowSpec spec)
  {
    this.buffer = buffer;
    this.spec = spec;
    this.macroTable = macroTable;
  }

  public Projector build(BatchWriter<?> output)
  {
    buildOffsetReaders();
    int rowWidth = spec.columns.size();
    List<ScalarColumnReader> colReaders = new ArrayList<>(rowWidth);
    for (int i = 0; i < rowWidth; i++) {
      colReaders.add(buildProjection(spec.columns.get(i)));
    }
    List<PartitionReader> readers = new ArrayList<>();
    primaryReader = buffer.primaryReader();
    readers.add(primaryReader);
    RowCursor inputCursor = primaryReader.cursor();
    if (!offsetReaders.isEmpty()) {
      List<RowCursor> followers =
          offsetReaders.values().stream()
            .map(reader -> reader.cursor())
            .collect(Collectors.toList());
      inputCursor = new PartitionCursor(inputCursor, followers);
      readers.addAll(offsetReaders.values());
    }
    return new Projector(inputCursor, readers, output, colReaders);
  }

  private void buildOffsetReaders()
  {
    for (OutputColumn col : spec.columns) {
      if (!(col instanceof OffsetExpression)) {
        continue;
      }
      OffsetExpression offsetProj = (OffsetExpression) col;
      offsetReaders.computeIfAbsent(offsetProj.offset, n -> (buffer.offsetReader(n)));
    }
  }

  private ScalarColumnReader buildProjection(OutputColumn outputColumn)
  {
    ColumnSchema colSchema = schemaBuilder.addScalar(outputColumn.name, outputColumn.dataType);
    if (outputColumn instanceof SimpleExpression) {
      return buildSimpleExpression((SimpleExpression) outputColumn, colSchema);
    }
    if (outputColumn instanceof OffsetExpression) {
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
      PartitionReader sourceReader = offsetReaders.get(exprCol.offset);
      ScalarColumnReader sourceCol = sourceReader.columns().scalar(expr.getIdentifierIfIdentifier());
      Preconditions.checkNotNull(sourceCol);
      return sourceCol;
    }
    throw new UOE("Invalid expression: [%s]", exprCol.expression);
  }
}
