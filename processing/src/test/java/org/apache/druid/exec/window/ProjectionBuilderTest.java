package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.batch.Batches;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.exec.plan.WindowSpec.OutputColumn;
import org.apache.druid.exec.plan.WindowSpec.CopyProjection;
import org.apache.druid.exec.plan.WindowSpec.OffsetExpression;
import org.apache.druid.exec.test.BatchBuilder;
import org.apache.druid.exec.test.SimpleDataGenOperator;
import org.apache.druid.exec.test.SimpleDataGenSpec;
import org.apache.druid.exec.test.TestUtils;
import org.apache.druid.exec.util.BatchValidator;
import org.apache.druid.exec.util.BatchVisualizer;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ProjectionBuilderTest
{
  private SimpleDataGenSpec dataGenSpec(int batchSize, int rowCount)
  {
    return new SimpleDataGenSpec(
        1,
        Arrays.asList("rid"),
        BatchFormat.OBJECT_ARRAY,
        batchSize,
        rowCount
    );
  }

  private BatchOperator dataGen(int batchSize, int rowCount)
  {
    return new SimpleDataGenOperator(
        TestUtils.emptyFragment(),
        dataGenSpec(batchSize, rowCount)
    );
  }

  private Batch runSequencer(ProjectionBuilder builder, BatchType batchType, WindowSpec spec)
  {
    WindowFrameSequencer sequencer = builder.build();
    BatchWriter<?> outputWriter = batchType.newWriter(builder.schema(), spec.batchSize);
    RowWriter rowWriter = outputWriter.rowWriter(builder.columnReaders());
    outputWriter.newBatch();
    while (sequencer.next()) {
      rowWriter.write();
    }
    return outputWriter.harvestAsBatch();
  }

  @Test
  public void testPrimaryOnly()
  {
    List<OutputColumn> cols = Collections.singletonList(
        new CopyProjection("rid", ColumnType.LONG, "rid")
    );
    WindowSpec spec = new WindowSpec(
        1,    // ID - not used
        2,    // Child - not used
        100,  // Batch Size - not used
        cols, // Output columns
        null  // Partition keys
    );
    final ExprMacroTable macroTable = ExprMacroTable.nil();
    final int rowCount = 4;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    ProjectionBuilder builder = new ProjectionBuilder(buffer, macroTable, spec);
    Batch output = runSequencer(builder, op.batchSchema().type(), spec);

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rid-2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1)
        .row(2)
        .row(3)
        .row(4)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  @Test
  public void testLeadOnly()
  {
    List<OutputColumn> cols = Arrays.<OutputColumn>asList(
        new CopyProjection("rid", ColumnType.LONG, "rid"),
        new OffsetExpression("rid+2", ColumnType.LONG, "rid", 2)
    );
    WindowSpec spec = new WindowSpec(
        1,    // ID - not used
        2,    // Child - not used
        100,  // Batch Size - not used
        cols, // Output columns
        null  // Partition keys
    );
    final ExprMacroTable macroTable = ExprMacroTable.nil();
    final int rowCount = 4;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    ProjectionBuilder builder = new ProjectionBuilder(buffer, macroTable, spec);
    Batch output = runSequencer(builder, op.batchSchema().type(), spec);

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rid+2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, 3)
        .row(2, 4)
        .row(3, null)
        .row(4, null)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  @Test
  public void testLagOnly()
  {
    List<OutputColumn> cols = Arrays.<OutputColumn>asList(
        new CopyProjection("rid", ColumnType.LONG, "rid"),
        new OffsetExpression("rid-2", ColumnType.LONG, "rid", -2)
    );
    WindowSpec spec = new WindowSpec(
        1,    // ID - not used
        2,    // Child - not used
        100,  // Batch Size - not used
        cols, // Output columns
        null  // Partition keys
    );
    final ExprMacroTable macroTable = ExprMacroTable.nil();
    final int rowCount = 4;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    ProjectionBuilder builder = new ProjectionBuilder(buffer, macroTable, spec);
    Batch output = runSequencer(builder, op.batchSchema().type(), spec);

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rid-2", ColumnType.LONG)
        .scalar("rid+2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, null)
        .row(2, null)
        .row(3, 1)
        .row(4, 2)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  @Test
  public void testPrimaryLeadAndLag()
  {
    List<OutputColumn> cols = Arrays.<OutputColumn>asList(
        new CopyProjection("rid", ColumnType.LONG, "rid"),
        new OffsetExpression("rid-2", ColumnType.LONG, "rid", -2),
        new OffsetExpression("rid+2", ColumnType.LONG, "rid", 2)
    );
    WindowSpec spec = new WindowSpec(
        1,    // ID - not used
        2,    // Child - not used
        100,  // Batch Size - not used
        cols, // Output columns
        null  // Partition keys
    );
    final ExprMacroTable macroTable = ExprMacroTable.nil();
    final int rowCount = 4;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    ProjectionBuilder builder = new ProjectionBuilder(buffer, macroTable, spec);
    Batch output = runSequencer(builder, op.batchSchema().type(), spec);

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rid-2", ColumnType.LONG)
        .scalar("rid+2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, null, 3)
        .row(2, null, 4)
        .row(3, 1, null)
        .row(4, 2, null)
        .build();
    BatchVisualizer.print(output);
    BatchValidator.assertEquals(expected, output);
  }
}
